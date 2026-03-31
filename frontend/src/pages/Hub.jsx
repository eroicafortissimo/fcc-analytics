import { Link } from 'react-router-dom'

export default function Hub() {
  return (
    <div className="min-h-screen bg-[#f8fafc]">
      {/* Header */}
      <div className="bg-[#1a2744] text-white py-14 px-8 text-center">
        <h1 className="text-4xl font-bold tracking-tight mb-3">FCC Analytics Dashboard</h1>
        <p className="text-blue-200 text-lg max-w-xl mx-auto">
          Intelligent sanctions and AML compliance tooling.
        </p>
      </div>

      <div className="max-w-5xl mx-auto px-6 py-12 space-y-10">

        {/* Modules */}
        <div>
          <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-4 text-center">Modules</h2>
          <div className="flex flex-col md:flex-row justify-center gap-5 max-w-2xl mx-auto">

            {/* Sanctions Module */}
            <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6 flex flex-col flex-1">
              <div className="mb-3">
                <h3 className="text-xl font-bold text-slate-900">Sanctions Module</h3>
              </div>
              <p className="text-sm text-slate-500 flex-1 mb-5">
                Validate your sanctions screening system. Download global watchlists, explore transaction
                distributions, generate test cases, interpret results, reconcile lists, and track daily
                watchlist updates with the List Update Manager.
              </p>
              <Link
                to="/screeniq"
                className="block text-center bg-[#1a2744] hover:bg-[#243560] text-white text-sm font-medium px-4 py-2.5 rounded-lg transition-colors"
              >
                Open →
              </Link>
            </div>

            {/* AML Module */}
            <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6 flex flex-col flex-1">
              <div className="mb-3">
                <h3 className="text-xl font-bold text-slate-900">AML Module</h3>
              </div>
              <p className="text-sm text-slate-500 flex-1 mb-5">
                AML analytics toolkit. Segment customers by risk profile, calibrate monitoring thresholds,
                and analyse above-the-line and below-the-line alert distributions.
              </p>
              <Link
                to="/amliq"
                className="block text-center bg-teal-800 hover:bg-teal-700 text-white text-sm font-medium px-4 py-2.5 rounded-lg transition-colors"
              >
                Open →
              </Link>
            </div>

          </div>
        </div>

        <p className="text-xs text-center text-slate-400">
          FCC Analytics Dashboard · React · FastAPI · Claude API · SQLite
        </p>
      </div>
    </div>
  )
}
