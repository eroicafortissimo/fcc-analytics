export default function RiskTypologyCoverage() {
  return (
    <div className="max-w-4xl mx-auto py-10">
      <div className="text-center mb-8">
        <h1 className="text-3xl font-bold text-slate-900 mb-3">Risk Typology Coverage Assessment</h1>
        <p className="text-slate-500 max-w-lg mx-auto text-sm leading-relaxed">
          Assess the coverage of your AML monitoring programme against known risk typologies and regulatory expectations.
        </p>
      </div>

      <div className="bg-white border border-slate-200 rounded-2xl p-12 flex flex-col items-center justify-center text-center">
        <div className="w-14 h-14 rounded-full bg-teal-50 border border-teal-200 flex items-center justify-center mb-4">
          <span className="text-2xl">🔬</span>
        </div>
        <h2 className="text-lg font-semibold text-slate-700 mb-2">Coming Soon</h2>
        <p className="text-sm text-slate-400 max-w-sm">
          This module is under development. It will allow you to map your monitoring rules against
          FATF typologies, regulatory guidance, and internal risk appetite.
        </p>
      </div>
    </div>
  )
}
