import { Link } from 'react-router-dom'

function SectionDivider({ label, color }) {
  return (
    <div className="flex items-center gap-3">
      <span className={`w-2.5 h-2.5 rounded-sm shrink-0 ${color}`} />
      <span className="text-xs font-bold text-slate-600 uppercase tracking-widest whitespace-nowrap">
        {label}
      </span>
      <div className="flex-1 h-px bg-slate-200" />
    </div>
  )
}

function ToolCard({ to, title, description, accent }) {
  return (
    <Link
      to={to}
      className={`bg-white rounded-xl border border-slate-200 border-l-4 ${accent}
                  p-6 flex flex-col hover:shadow-md transition-shadow group`}
    >
      <h3 className="text-base font-semibold text-slate-800 mb-2 group-hover:text-slate-900">
        {title}
      </h3>
      <p className="text-slate-500 text-sm flex-1 leading-relaxed mb-4">{description}</p>
      <span className="text-xs font-medium text-slate-400 group-hover:text-slate-600 transition-colors">
        Open →
      </span>
    </Link>
  )
}

export default function Home() {
  return (
    <div className="max-w-4xl mx-auto py-10 space-y-7">

      {/* Header */}
      <div className="text-center mb-4">
        <h1 className="text-4xl font-bold text-slate-900 mb-3">Sanctions Module</h1>
        <p className="text-slate-500 max-w-lg mx-auto text-sm leading-relaxed">
          Sanctions screening validation toolkit — explore watchlists and transactions,
          generate test cases, interpret results, and reconcile public and private lists.
        </p>
      </div>

      {/* Explorer */}
      <div className="space-y-4">
        <SectionDivider label="Explorer" color="bg-blue-600" />
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <ToolCard
            to="/screeniq/lists"
            title="Watchlist Explorer"
            description="Download and analyze OFAC SDN, EU Consolidated, UK HMT, BIS Entity List, and Japan METI sanctions lists with nationality inference, filters, and interactive charts."
            accent="border-l-blue-600"
          />
          <ToolCard
            to="/screeniq/transactions"
            title="Transaction Explorer"
            description="Upload a CSV or Excel file of transaction names and entities to analyze distributions of countries, entity types, token counts, and name lengths."
            accent="border-l-blue-600"
          />
        </div>
      </div>

      {/* Tuning Tools */}
      <div className="space-y-4">
        <SectionDivider label="Tuning Tools" color="bg-red-500" />
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <ToolCard
            to="/screeniq/testcases"
            title="Test Case Generator"
            description="Generate hundreds of intelligent name variation test cases — transpositions, truncations, transliterations, phonetic equivalents, and more. Export as Excel, SWIFT, or FUF."
            accent="border-l-red-500"
          />
          <ToolCard
            to="/screeniq/results"
            title="Results Interpreter"
            description="Upload your screening results for a full statistical analysis: confusion matrix, detection rate, false positive rate, breakdowns by type and culture, and AI-powered miss analysis."
            accent="border-l-red-500"
          />
        </div>
      </div>

      {/* List Management */}
      <div className="space-y-4">
        <SectionDivider label="List Management" color="bg-violet-600" />
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <ToolCard
            to="/screeniq/reconciliation"
            title="List Reconciliation"
            description="Upload your private screening list and reconcile it against public watchlists. Identify coverage gaps (public entries missing from your list) and private extras across three matching tiers: exact, expanded, and AI."
            accent="border-l-violet-600"
          />
          <ToolCard
            to="/screeniq/list-update-manager"
            title="List Update Manager"
            description="Track daily changes across OFAC and global sanctions watchlists. Surface additions, deletions, and modifications the moment they happen. Configure automated sync schedules and review full change history."
            accent="border-l-violet-600"
          />
        </div>
      </div>

      {/* False Positive Suppression */}
      <div className="space-y-4">
        <SectionDivider label="False Positive Suppression" color="bg-green-600" />
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <ToolCard
            to="/screeniq/rule-automation"
            title="Rules Manager"
            description="Create suppression rules from plain-language descriptions using AI, interpret existing rules to understand what they do, and generate detailed rule logs showing field impact and composition."
            accent="border-l-green-600"
          />
          <ToolCard
            to="/screeniq/good-guys"
            title="Good Guys Manager"
            description="Create and manage trusted entity and whitelist rules. Interpret existing good guys rule files, visualize rule inventories, and generate comprehensive logs of trusted entity rule composition."
            accent="border-l-green-600"
          />
        </div>
      </div>

      {/* Tech stack */}
      <p className="text-xs text-center text-slate-400 pt-2">
        React · FastAPI · LangChain / LangGraph · Claude API · SQLite · Recharts
      </p>

    </div>
  )
}
