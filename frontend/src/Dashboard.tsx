import { useState, useEffect } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  PointElement,
  LineElement,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  PointElement,
  LineElement
)

// Types for API responses
interface ScoresBucket {
  bucket: string
  count: number
}

interface PassRate {
  task: string
  avg_score: number
  attempts: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface DashboardData {
  scores: ScoresBucket[]
  passRates: PassRate[]
  timeline: TimelineEntry[]
}

interface DashboardProps {
  apiBaseUrl: string
  apiKey: string
}

const LAB_OPTIONS = [
  { value: 'lab-01', label: 'Lab 01' },
  { value: 'lab-02', label: 'Lab 02' },
  { value: 'lab-03', label: 'Lab 03' },
  { value: 'lab-04', label: 'Lab 04' },
  { value: 'lab-05', label: 'Lab 05' },
]

export default function Dashboard({ apiBaseUrl, apiKey }: DashboardProps) {
  const [selectedLab, setSelectedLab] = useState<string>('lab-04')
  const [data, setData] = useState<DashboardData | null>(null)
  const [loading, setLoading] = useState<boolean>(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchDashboardData = async () => {
      setLoading(true)
      setError(null)

      try {
        const headers = {
          Authorization: `Bearer ${apiKey}`,
          Accept: 'application/json',
        }

        const [scoresRes, passRatesRes, timelineRes] = await Promise.all([
          fetch(`${apiBaseUrl}/analytics/scores?lab=${selectedLab}`, { headers }),
          fetch(`${apiBaseUrl}/analytics/pass-rates?lab=${selectedLab}`, { headers }),
          fetch(`${apiBaseUrl}/analytics/timeline?lab=${selectedLab}`, { headers }),
        ])

        if (!scoresRes.ok) throw new Error(`Scores: HTTP ${scoresRes.status}`)
        if (!passRatesRes.ok) throw new Error(`Pass rates: HTTP ${passRatesRes.status}`)
        if (!timelineRes.ok) throw new Error(`Timeline: HTTP ${timelineRes.status}`)

        const scores: ScoresBucket[] = await scoresRes.json()
        const passRates: PassRate[] = await passRatesRes.json()
        const timeline: TimelineEntry[] = await timelineRes.json()

        setData({ scores, passRates, timeline })
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error')
      } finally {
        setLoading(false)
      }
    }

    fetchDashboardData()
  }, [selectedLab, apiBaseUrl, apiKey])

  // Prepare chart data for scores bar chart
  const scoresChartData = {
    labels: data?.scores.map((s) => s.bucket) ?? [],
    datasets: [
      {
        label: 'Number of Students',
        data: data?.scores.map((s) => s.count) ?? [],
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  }

  const scoresChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
      title: {
        display: true,
        text: 'Score Distribution',
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        ticks: {
          stepSize: 1,
        },
      },
    },
  }

  // Prepare chart data for timeline line chart
  const timelineChartData = {
    labels: data?.timeline.map((t) => t.date) ?? [],
    datasets: [
      {
        label: 'Submissions',
        data: data?.timeline.map((t) => t.submissions) ?? [],
        backgroundColor: 'rgba(75, 192, 192, 0.6)',
        borderColor: 'rgba(75, 192, 192, 1)',
        borderWidth: 2,
        tension: 0.3,
      },
    ],
  }

  const timelineChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
      title: {
        display: true,
        text: 'Submissions Over Time',
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        ticks: {
          stepSize: 1,
        },
      },
    },
  }

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>Analytics Dashboard</h1>
        <div className="lab-selector">
          <label htmlFor="lab-select">Select Lab: </label>
          <select
            id="lab-select"
            value={selectedLab}
            onChange={(e) => setSelectedLab(e.target.value)}
          >
            {LAB_OPTIONS.map((lab) => (
              <option key={lab.value} value={lab.value}>
                {lab.label}
              </option>
            ))}
          </select>
        </div>
      </header>

      {loading && <p className="loading">Loading dashboard data...</p>}
      {error && <p className="error">Error: {error}</p>}

      {data && !loading && (
        <div className="dashboard-content">
          <div className="chart-container">
            <Bar data={scoresChartData} options={scoresChartOptions} />
          </div>

          <div className="chart-container">
            <Line data={timelineChartData} options={timelineChartOptions} />
          </div>

          <div className="table-container">
            <h2>Pass Rates by Task</h2>
            {data.passRates.length > 0 ? (
              <table className="pass-rates-table">
                <thead>
                  <tr>
                    <th>Task</th>
                    <th>Avg Score (%)</th>
                    <th>Attempts</th>
                  </tr>
                </thead>
                <tbody>
                  {data.passRates.map((pr, index) => (
                    <tr key={index}>
                      <td>{pr.task}</td>
                      <td>{pr.avg_score.toFixed(1)}</td>
                      <td>{pr.attempts}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p>No pass rate data available for this lab.</p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
