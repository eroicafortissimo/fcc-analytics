import { Routes, Route, Link, useLocation } from 'react-router-dom'
import Home from './pages/Home.jsx'
import ListExplorer from './pages/ListExplorer.jsx'
import TestCaseGenerator from './pages/TestCaseGenerator.jsx'
import ResultsInterpreter from './pages/ResultsInterpreter.jsx'

const NAV_LINKS = [
  { to: '/', label: 'Home' },
  { to: '/lists', label: 'List Explorer' },
  { to: '/testcases', label: 'Test Cases' },
  { to: '/results', label: 'Results' },
]

function NavBar() {
  const { pathname } = useLocation()
  return (
    <nav className="bg-slate-900 text-white px-6 py-3 flex items-center gap-6 shadow-md">
      <span className="font-semibold text-lg tracking-tight mr-4">
        Screening Validation Platform
      </span>
      {NAV_LINKS.map(({ to, label }) => (
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

export default function App() {
  return (
    <div className="min-h-screen bg-slate-50 flex flex-col">
      <NavBar />
      <main className="flex-1 p-6">
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/lists" element={<ListExplorer />} />
          <Route path="/testcases" element={<TestCaseGenerator />} />
          <Route path="/results" element={<ResultsInterpreter />} />
        </Routes>
      </main>
    </div>
  )
}
