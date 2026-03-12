"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, case, distinct, and_
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    - Find the lab item by matching title (e.g. "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    # Transform lab parameter to match title format (e.g., "lab-04" → "Lab 04")
    lab_title = lab.replace("lab-", "Lab ").replace("-", " ")

    # Find the lab item
    lab_result = await session.execute(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(lab_title)
        )
    )
    lab_item = lab_result.scalar_one_or_none()

    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Find all tasks that belong to this lab
    tasks_result = await session.execute(
        select(ItemRecord.id).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        )
    )
    task_ids = [row[0] for row in tasks_result.all()]

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Query interactions and group by score buckets
    bucket_query = select(
        func.sum(
            case(
                (InteractionLog.score <= 25, 1),
                else_=0
            )
        ).label("count_0_25"),
        func.sum(
            case(
                (and_(InteractionLog.score > 25, InteractionLog.score <= 50), 1),
                else_=0
            )
        ).label("count_26_50"),
        func.sum(
            case(
                (and_(InteractionLog.score > 50, InteractionLog.score <= 75), 1),
                else_=0
            )
        ).label("count_51_75"),
        func.sum(
            case(
                (InteractionLog.score > 75, 1),
                else_=0
            )
        ).label("count_76_100"),
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    )

    result = await session.execute(bucket_query)
    row = result.one()

    return [
        {"bucket": "0-25", "count": row.count_0_25 or 0},
        {"bucket": "26-50", "count": row.count_26_50 or 0},
        {"bucket": "51-75", "count": row.count_51_75 or 0},
        {"bucket": "76-100", "count": row.count_76_100 or 0},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    # Transform lab parameter to match title format
    lab_title = lab.replace("lab-", "Lab ").replace("-", " ")

    # Find the lab item
    lab_result = await session.execute(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(lab_title)
        )
    )
    lab_item = lab_result.scalar_one_or_none()

    if not lab_item:
        return []

    # Find all tasks that belong to this lab
    tasks_result = await session.execute(
        select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        ).order_by(ItemRecord.title)
    )
    tasks = tasks_result.scalars().all()

    results = []
    for task in tasks:
        # Compute avg_score and attempts for this task
        stats_query = select(
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        ).where(
            InteractionLog.item_id == task.id,
            InteractionLog.score.isnot(None)
        )

        stats_result = await session.execute(stats_query)
        stats_row = stats_result.one()

        results.append({
            "task": task.title,
            "avg_score": float(stats_row.avg_score) if stats_row.avg_score is not None else 0.0,
            "attempts": stats_row.attempts or 0,
        })

    return results


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    # Transform lab parameter to match title format
    lab_title = lab.replace("lab-", "Lab ").replace("-", " ")

    # Find the lab item
    lab_result = await session.execute(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(lab_title)
        )
    )
    lab_item = lab_result.scalar_one_or_none()

    if not lab_item:
        return []

    # Find all tasks that belong to this lab
    tasks_result = await session.execute(
        select(ItemRecord.id).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        )
    )
    task_ids = [row[0] for row in tasks_result.all()]

    if not task_ids:
        return []

    # Group interactions by date
    timeline_query = select(
        func.date(InteractionLog.created_at).label("date"),
        func.count(InteractionLog.id).label("submissions"),
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(
        func.date(InteractionLog.created_at)
    ).order_by(
        func.date(InteractionLog.created_at).asc()
    )

    result = await session.execute(timeline_query)
    rows = result.all()

    return [
        {"date": str(row.date), "submissions": row.submissions}
        for row in rows
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    # Transform lab parameter to match title format
    lab_title = lab.replace("lab-", "Lab ").replace("-", " ")

    # Find the lab item
    lab_result = await session.execute(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(lab_title)
        )
    )
    lab_item = lab_result.scalar_one_or_none()

    if not lab_item:
        return []

    # Find all tasks that belong to this lab
    tasks_result = await session.execute(
        select(ItemRecord.id).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        )
    )
    task_ids = [row[0] for row in tasks_result.all()]

    if not task_ids:
        return []

    # Join interactions with learners and group by student_group
    groups_query = select(
        Learner.student_group.label("group"),
        func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
        func.count(distinct(Learner.id)).label("students"),
    ).join(
        InteractionLog, InteractionLog.learner_id == Learner.id
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by(
        Learner.student_group
    ).order_by(
        Learner.student_group
    )

    result = await session.execute(groups_query)
    rows = result.all()

    return [
        {
            "group": row.group,
            "avg_score": float(row.avg_score) if row.avg_score is not None else 0.0,
            "students": row.students,
        }
        for row in rows
    ]
