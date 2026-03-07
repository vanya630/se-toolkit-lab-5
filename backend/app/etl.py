"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlalchemy import func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    all_logs: list[dict] = []
    current_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                params=params,
                auth=(settings.autochecker_email, settings.autochecker_password),
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Use the last log's submitted_at as the new since value
            if logs:
                last_log = logs[-1]
                current_since = datetime.fromisoformat(last_log["submitted_at"])
            else:
                break

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> tuple[int, dict[tuple[str, str | None], int]]:
    """Load items (labs and tasks) into the database.

    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items and a mapping from (lab, task) to item_id
    """
    new_count = 0
    lab_id_map: dict[str, ItemRecord] = {}
    item_id_map: dict[tuple[str, str | None], int] = {}

    # Process labs first
    for item in items:
        if item["type"] != "lab":
            continue

        # Check if lab already exists by title
        existing = await session.execute(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == item["title"],
            )
        )
        lab_record = existing.scalar_one_or_none()

        if lab_record is None:
            lab_record = ItemRecord(type="lab", title=item["title"])
            session.add(lab_record)
            new_count += 1

        # Map lab short ID to record
        lab_id_map[item["lab"]] = lab_record
        # Map (lab, None) to lab item id
        item_id_map[(item["lab"], None)] = lab_record.id

    # Process tasks
    for item in items:
        if item["type"] != "task":
            continue

        # Find parent lab by short ID
        parent_lab = lab_id_map.get(item["lab"])
        if parent_lab is None:
            # Parent lab might not exist in this fetch, skip task
            continue

        # Check if task already exists by title and parent_id
        existing = await session.execute(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == item["title"],
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        task_record = existing.scalar_one_or_none()

        if task_record is None:
            task_record = ItemRecord(
                type="task",
                title=item["title"],
                parent_id=parent_lab.id,
            )
            session.add(task_record)
            new_count += 1

        # Map (lab, task) to task item id
        item_id_map[(item["lab"], item["task"])] = task_record.id

    await session.commit()
    return new_count, item_id_map


async def load_logs(
    logs: list[dict], item_id_map: dict[tuple[str, str | None], int], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        item_id_map: Mapping from (lab_short_id, task_short_id) to item_id
        session: Database session.

    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database using item_id_map
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    new_count = 0

    for log in logs:
        # 1. Find or create learner
        learner = await session.execute(
            select(Learner).where(
                Learner.external_id == log["student_id"]
            )
        )
        learner_record = learner.scalar_one_or_none()

        if learner_record is None:
            learner_record = Learner(
                external_id=log["student_id"],
                student_group=log.get("group", "unknown"),
            )
            session.add(learner_record)
            # Flush to get the ID
            await session.flush()

        # 2. Find matching item using item_id_map
        task_key = (log["lab"], log.get("task"))
        item_id = item_id_map.get(task_key)

        if item_id is None:
            # No matching item found, skip this log
            continue

        # 3. Check if interaction log already exists (idempotency)
        existing_log = await session.execute(
            select(InteractionLog).where(
                InteractionLog.external_id == log["id"]
            )
        )
        if existing_log.scalar_one_or_none() is not None:
            # Already exists, skip
            continue

        # 4. Create new interaction log
        interaction_log = InteractionLog(
            external_id=log["id"],
            learner_id=learner_record.id,
            item_id=item_id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=datetime.fromisoformat(log["submitted_at"]),
        )
        session.add(interaction_log)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the item_id_map to load_logs so it can map (lab, task) to item IDs
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    # Step 1: Fetch and load items
    items = await fetch_items()
    _, item_id_map = await load_items(items, session)

    # Step 2: Get last synced timestamp
    last_log = await session.execute(
        select(InteractionLog)
        .order_by(InteractionLog.created_at.desc())
        .limit(1)
    )
    last_record = last_log.scalar_one_or_none()
    since = last_record.created_at if last_record else None

    # Step 3: Fetch and load logs
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, item_id_map, session)

    # Get total count
    total_result = await session.execute(
        select(func.count(InteractionLog.id))
    )
    total_records = total_result.scalar_one() or 0

    return {"new_records": new_records, "total_records": total_records}
