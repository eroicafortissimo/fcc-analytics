import { Link } from 'react-router-dom'

const FEATURES = [
  {
    to: '/lists',
    title: 'List Explorer',
    icon: '🗂',
    description:
      'Download and analyze OFAC SDN, EU Consolidated, UK HMT, BIS Entity List, and Japan METI sanctions lists. ' +
      'Browse entries with intelligent nationality inference, filters, and interactive charts.',
    color: 'border-blue-500',
    btn: 'bg-blue-600 hover:bg-blue-700',
  },
  {
    to: '/testcases',
    title: 'Test Case Generator',
    icon: '🧪',
    description:
      'Generate hundreds of intelligent name variation test cases — transpositions, truncations, transliterations, ' +
      'phonetic equivalents, and more — with expected HIT/MISS results. Export as Excel, SWIFT pacs.008/009, or FUF.',
    color: 'border-green-500',
    btn: 'bg-green-600 hover:bg-green-700',
  },
  {
    to: '/results',
    title: 'Results Interpreter',
    icon: '📊',
    description:
      'Upload your screening system\'s results and get a full statistical analysis: confusion matrix, detection rate, ' +
      'false positive rate, breakdowns by test type and culture, plus AI-powered miss analysis for false negatives.',
    color: 'border-purple-500',
    btn: 'bg-purple-600 hover:bg-purple-700',
  },
]

export default function Home() {
  return (
    <div className="max-w-5xl mx-auto py-10">
      <div className="text-center mb-12">
        <h1 className="text-4xl font-bold text-slate-900 mb-4">
          Screening Validation Platform
        </h1>
        <p className="text-lg text-slate-600 max-w-2xl mx-auto">
          A full-stack sanctions screening validation toolkit. Download and explore global watchlists,
          generate intelligent test cases covering hundreds of name variation types, then analyze
          your screening system's performance to identify gaps and biases.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {FEATURES.map(({ to, title, icon, description, color, btn }) => (
          <div
            key={to}
            className={`bg-white rounded-xl border-t-4 ${color} shadow-sm p-6 flex flex-col`}
          >
            <div className="text-4xl mb-3">{icon}</div>
            <h2 className="text-xl font-semibold text-slate-800 mb-2">{title}</h2>
            <p className="text-slate-500 text-sm flex-1 mb-5">{description}</p>
            <Link
              to={to}
              className={`${btn} text-white text-sm font-medium px-4 py-2 rounded-lg text-center transition-colors`}
            >
              Open {title}
            </Link>
          </div>
        ))}
      </div>

      <div className="mt-12 bg-slate-100 rounded-xl p-6 text-sm text-slate-500">
        <strong className="text-slate-700">Tech stack:</strong>{' '}
        React · FastAPI · LangChain / LangGraph · Claude API · SQLite · Recharts
      </div>
    </div>
  )
}
