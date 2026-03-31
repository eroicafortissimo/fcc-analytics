import { Routes, Route, Link, useLocation } from 'react-router-dom'
import Hub from './pages/Hub.jsx'
import Home from './pages/Home.jsx'
import ListExplorer from './pages/ListExplorer.jsx'
import TestCaseGenerator from './pages/TestCaseGenerator.jsx'
import ResultsInterpreter from './pages/ResultsInterpreter.jsx'
import TransactIQ from './pages/TransactIQ.jsx'
import ListReconciliation from './pages/ListReconciliation.jsx'
import ListIQDashboard from './pages/listiq/ListIQDashboard.jsx'
import ListIQSettings from './pages/listiq/ListIQSettings.jsx'
import AMLIQHome from './pages/amliq/AMLIQHome.jsx'
import CustomerSegmentation from './pages/amliq/CustomerSegmentation.jsx'
import ThresholdSetting from './pages/amliq/ThresholdSetting.jsx'
import ATLBTL from './pages/amliq/ATLBTL.jsx'
import RiskTypologyCoverage from './pages/amliq/RiskTypologyCoverage.jsx'

const SCREENIQ_NAV = [
  { to: '/screeniq', label: 'Home' },
  { to: '/screeniq/lists', label: 'Watchlist Explorer' },
  { to: '/screeniq/transactions', label: 'Transaction Explorer' },
  { to: '/screeniq/testcases', label: 'Test Case Generator' },
  { to: '/screeniq/results', label: 'Results Interpreter' },
  { to: '/screeniq/reconciliation', label: 'List Reconciliation' },
  { to: '/screeniq/list-update-manager', label: 'List Update Manager' },
]

const AMLIQ_NAV = [
  { to: '/amliq', label: 'Home' },
  { to: '/amliq/segmentation', label: 'Customer Segmentation' },
  { to: '/amliq/thresholds', label: 'Threshold Setting' },
  { to: '/amliq/atlbtl', label: 'ATL / BTL' },
  { to: '/amliq/risk-typology', label: 'Risk Typology Coverage' },
]

function ScreenIQNav() {
  const { pathname } = useLocation()
  return (
    <nav className="bg-slate-900 text-white px-6 py-3 flex items-center gap-6 shadow-md flex-wrap">
      <Link to="/" className="text-slate-400 hover:text-white text-xs mr-1 transition-colors">
        ← FCC Analytics Dashboard
      </Link>
      <span className="text-slate-600">|</span>
      <span className="font-semibold text-lg tracking-tight mr-2">Sanctions Module</span>
      {SCREENIQ_NAV.map(({ to, label }) => (
        <Link
          key={to}
          to={to}
          className={`text-sm px-3 py-1 rounded transition-colors ${
            pathname === to
              ? 'bg-blue-600 text-white'
              : 'text-slate-300 hover:text-white hover:bg-slate-700'
          }`}
        >
          {label}
        </Link>
      ))}
    </nav>
  )
}

function AMLIQNav() {
  const { pathname } = useLocation()
  return (
    <nav className="bg-teal-900 text-white px-6 py-3 flex items-center gap-6 shadow-md flex-wrap">
      <Link to="/" className="text-teal-300 hover:text-white text-xs mr-1 transition-colors">
        ← FCC Analytics Dashboard
      </Link>
      <span className="text-teal-700">|</span>
      <span className="font-semibold text-lg tracking-tight mr-2">AML Module</span>
      {AMLIQ_NAV.map(({ to, label }) => (
        <Link
          key={to}
          to={to}
          className={`text-sm px-3 py-1 rounded transition-colors ${
            pathname === to
              ? 'bg-teal-600 text-white'
              : 'text-teal-200 hover:text-white hover:bg-teal-800'
          }`}
        >
          {label}
        </Link>
      ))}
    </nav>
  )
}

function AppLayout({ nav, children }) {
  return (
    <div className="min-h-screen bg-slate-100 flex flex-col">
      {nav}
      <main className="flex-1 p-6">{children}</main>
    </div>
  )
}

export default function App() {
  return (
    <Routes>
      {/* Hub — no nav bar */}
      <Route path="/" element={<Hub />} />

      {/* ScreenIQ */}
      <Route path="/screeniq" element={
        <AppLayout nav={<ScreenIQNav />}><Home /></AppLayout>
      } />
      <Route path="/screeniq/lists" element={
        <AppLayout nav={<ScreenIQNav />}><ListExplorer /></AppLayout>
      } />
      <Route path="/screeniq/transactions" element={
        <AppLayout nav={<ScreenIQNav />}><TransactIQ /></AppLayout>
      } />
      <Route path="/screeniq/testcases" element={
        <AppLayout nav={<ScreenIQNav />}><TestCaseGenerator /></AppLayout>
      } />
      <Route path="/screeniq/results" element={
        <AppLayout nav={<ScreenIQNav />}><ResultsInterpreter /></AppLayout>
      } />
      <Route path="/screeniq/reconciliation" element={
        <AppLayout nav={<ScreenIQNav />}><ListReconciliation /></AppLayout>
      } />
      <Route path="/screeniq/list-update-manager" element={
        <AppLayout nav={<ScreenIQNav />}><ListIQDashboard /></AppLayout>
      } />
      <Route path="/screeniq/list-update-manager/settings" element={
        <AppLayout nav={<ScreenIQNav />}><ListIQSettings /></AppLayout>
      } />

      {/* AMLIQ */}
      <Route path="/amliq" element={
        <AppLayout nav={<AMLIQNav />}><AMLIQHome /></AppLayout>
      } />
      <Route path="/amliq/segmentation" element={
        <AppLayout nav={<AMLIQNav />}><CustomerSegmentation /></AppLayout>
      } />
      <Route path="/amliq/thresholds" element={
        <AppLayout nav={<AMLIQNav />}><ThresholdSetting /></AppLayout>
      } />
      <Route path="/amliq/atlbtl" element={
        <AppLayout nav={<AMLIQNav />}><ATLBTL /></AppLayout>
      } />
      <Route path="/amliq/risk-typology" element={
        <AppLayout nav={<AMLIQNav />}><RiskTypologyCoverage /></AppLayout>
      } />
    </Routes>
  )
}
