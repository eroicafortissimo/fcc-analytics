import { useState, useRef, useEffect } from 'react'
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
import RuleAutomation from './pages/screeniq/RuleAutomation.jsx'
import GoodGuysAutomation from './pages/screeniq/GoodGuysAutomation.jsx'
import AMLIQHome from './pages/amliq/AMLIQHome.jsx'
import CustomerSegmentation from './pages/amliq/CustomerSegmentation.jsx'
import ThresholdSetting from './pages/amliq/ThresholdSetting.jsx'
import ATLBTL from './pages/amliq/ATLBTL.jsx'
import RiskTypologyCoverage from './pages/amliq/RiskTypologyCoverage.jsx'

const SCREENIQ_GROUPS = [
  {
    label: 'Explorer',
    items: [
      { to: '/screeniq/lists', label: 'Watchlist Explorer' },
      { to: '/screeniq/transactions', label: 'Transaction Explorer' },
    ],
  },
  {
    label: 'Tuning Tools',
    items: [
      { to: '/screeniq/testcases', label: 'Test Case Generator' },
      { to: '/screeniq/results', label: 'Results Interpreter' },
    ],
  },
  {
    label: 'List Management',
    items: [
      { to: '/screeniq/reconciliation', label: 'List Reconciliation' },
      { to: '/screeniq/list-update-manager', label: 'List Update Manager' },
    ],
  },
  {
    label: 'FP Suppression',
    items: [
      { to: '/screeniq/rule-automation', label: 'Rules Manager' },
      { to: '/screeniq/good-guys', label: 'Good Guys Manager' },
    ],
  },
]

const AMLIQ_NAV = [
  { to: '/amliq', label: 'Home' },
  { to: '/amliq/segmentation', label: 'Customer Segmentation' },
  { to: '/amliq/thresholds', label: 'Threshold Setting' },
  { to: '/amliq/atlbtl', label: 'ATL / BTL' },
  { to: '/amliq/risk-typology', label: 'Risk Typology Coverage' },
]

function NavDropdown({ group, pathname }) {
  const [open, setOpen] = useState(false)
  const ref = useRef(null)
  const isActive = group.items.some(item => pathname === item.to || pathname.startsWith(item.to + '/'))

  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  return (
    <div
      ref={ref}
      className="relative"
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
    >
      <button
        className={`flex items-center gap-1 text-sm px-3 py-1 rounded transition-colors ${
          isActive ? 'bg-blue-600 text-white' : 'text-slate-300 hover:text-white hover:bg-slate-700'
        }`}
      >
        {group.label}
        <svg className="w-3 h-3 opacity-60" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <div className="absolute top-full left-0 mt-1 bg-white rounded-xl shadow-lg border border-slate-200 py-1 min-w-[200px] z-50">
          {group.items.map(item => (
            <Link
              key={item.to}
              to={item.to}
              onClick={() => setOpen(false)}
              className={`block px-4 py-2 text-sm transition-colors ${
                pathname === item.to || pathname.startsWith(item.to + '/')
                  ? 'bg-blue-50 text-blue-700 font-medium'
                  : 'text-slate-700 hover:bg-slate-50'
              }`}
            >
              {item.label}
            </Link>
          ))}
        </div>
      )}
    </div>
  )
}

function ScreenIQNav() {
  const { pathname } = useLocation()
  return (
    <nav className="bg-slate-900 text-white px-6 py-3 flex items-center gap-4 shadow-md">
      <Link to="/" className="text-slate-400 hover:text-white text-xs mr-1 transition-colors shrink-0">
        ← FCC Analytics Dashboard
      </Link>
      <span className="text-slate-600">|</span>
      <Link to="/screeniq" className={`font-semibold text-lg tracking-tight mr-2 shrink-0 hover:text-slate-200 transition-colors ${pathname === '/screeniq' ? 'text-white' : 'text-slate-100'}`}>
        Sanctions Module
      </Link>
      {SCREENIQ_GROUPS.map(group => (
        <NavDropdown key={group.label} group={group} pathname={pathname} />
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
      <Route path="/screeniq/rule-automation" element={
        <AppLayout nav={<ScreenIQNav />}><RuleAutomation /></AppLayout>
      } />
      <Route path="/screeniq/good-guys" element={
        <AppLayout nav={<ScreenIQNav />}><GoodGuysAutomation /></AppLayout>
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
