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

export default function AMLIQHome() {
  return (
    <div className="max-w-4xl mx-auto py-10 space-y-7">

      <div className="text-center mb-4">
        <h1 className="text-4xl font-bold text-slate-900 mb-3">AML Module</h1>
        <p className="text-slate-500 max-w-lg mx-auto text-sm leading-relaxed">
          AML analytics toolkit — segment customers, set intelligent thresholds,
          and analyse above-the-line and below-the-line alert distributions.
        </p>
      </div>

      <div className="space-y-4">
        <SectionDivider label="Analytics" color="bg-teal-600" />
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <ToolCard
            to="/amliq/segmentation"
            title="Customer Segmentation"
            description="Segment your customer population by risk profile, transaction behaviour, and entity type to build targeted AML monitoring strategies."
            accent="border-l-teal-600"
          />
          <ToolCard
            to="/amliq/thresholds"
            title="Threshold Setting"
            description="Analyse alert volumes and false positive rates to calibrate monitoring thresholds by segment, channel, and rule type."
            accent="border-l-teal-600"
          />
        </div>
      </div>

      <div className="space-y-4">
        <SectionDivider label="Risk Coverage" color="bg-violet-600" />
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <ToolCard
            to="/amliq/risk-typology"
            title="Risk Typology Coverage Assessment"
            description="Map your monitoring programme against FATF typologies, regulatory guidance, and internal risk appetite to identify coverage gaps."
            accent="border-l-violet-600"
          />
        </div>
      </div>

      <div className="space-y-4">
        <SectionDivider label="Alert Distribution" color="bg-cyan-600" />
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <ToolCard
            to="/amliq/atlbtl"
            title="ATL / BTL Analysis"
            description="Visualise the distribution of alerts above and below the line. Understand how threshold changes shift alert volumes and risk coverage."
            accent="border-l-cyan-600"
          />
        </div>
      </div>

      <p className="text-xs text-center text-slate-400 pt-2">
        React · FastAPI · Claude API · SQLite · Recharts
      </p>
    </div>
  )
}
