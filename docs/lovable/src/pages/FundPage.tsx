import { useState, useEffect, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { Sun, Moon } from 'lucide-react'
import { SearchBar } from '@/components/SearchBar'
import { FundHeader } from '@/components/FundHeader'
import { PriceCard } from '@/components/PriceCard'
import { VolatilityCard, LiquidityCard } from '@/components/MetricsCards'
import { HealthCard } from '@/components/HealthCard'
import { DividendCard } from '@/components/DividendCard'
import { SimulatorCard } from '@/components/SimulatorCard'
import { AIManagerCard } from '@/components/AIManagerCard'
import { type FundData, buildFundData } from '@/lib/fii-helpers'
import { useTheme } from '@/hooks/use-theme'
import logoLight from '@/assets/logo-light.svg'
import logoDark from '@/assets/logo-dark.svg'

const API_BASE = 'https://fii-prices.up.railway.app'

export default function FundPage() {
  const { ticker: tickerParam } = useParams<{ ticker: string }>()
  const navigate = useNavigate()
  const [fund, setFund] = useState<FundData | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const { isDark, toggle } = useTheme()

  // Toggle html[data-view] so the static homepage markup hides while FundPage is mounted
  useEffect(() => {
    document.documentElement.dataset.view = 'app'
    return () => { document.documentElement.dataset.view = 'home' }
  }, [])

  const loadFund = useCallback(async (ticker: string) => {
    ticker = ticker.trim().toUpperCase()
    if (!ticker) return
    setError('')
    setLoading(true)

    try {
      const [precoRes, divRes, informeRes, benchRes, cdiRes, gestorRes] = await Promise.all([
        fetch(`${API_BASE}/fundo/preco?ticker=${ticker}`),
        fetch(`${API_BASE}/fundo/dividendos?ticker=${ticker}`),
        fetch(`${API_BASE}/fundo/informe?ticker=${ticker}`),
        fetch(`${API_BASE}/benchmarks`),
        fetch(`${API_BASE}/cdi`),
        fetch(`${API_BASE}/fundo/gestor?ticker=${ticker}`),
      ])

      if (!precoRes.ok) {
        const err = await precoRes.json().catch(() => ({}))
        throw new Error((err as { detail?: string }).detail || `Fundo ${ticker} não encontrado`)
      }
      if (!divRes.ok) {
        const err = await divRes.json().catch(() => ({}))
        throw new Error((err as { detail?: string }).detail || `Dividendos para ${ticker} não encontrados`)
      }

      const preco    = await precoRes.json()
      const div      = await divRes.json()
      const informe   = informeRes.ok ? await informeRes.json()  : null
      const benchData = benchRes.ok   ? await benchRes.json()    : null
      const cdiData   = cdiRes.ok     ? await cdiRes.json()      : null
      const gestorData = gestorRes.ok ? await gestorRes.json()   : null

      const f = buildFundData(ticker, preco, div, informe, benchData, cdiData, gestorData)
      setFund(f)
      document.title = `${ticker} — FII Guia`
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  // Navigate to a new fund's URL; useEffect below handles the actual fetch.
  const onSearch = useCallback((ticker: string) => {
    const t = ticker.trim().toUpperCase()
    if (!t) return
    if (t !== tickerParam) navigate(`/fundo/${t}`)
  }, [navigate, tickerParam])

  // Load whenever the URL ticker changes.
  useEffect(() => {
    if (tickerParam) loadFund(tickerParam)
  }, [tickerParam, loadFund])

  return (
    <div className="min-h-screen bg-background transition-colors duration-300">
      {/* Header — matches the homepage */}
      <header className="sticky top-0 z-50 border-b border-border/60 bg-background/90 backdrop-blur-xl">
        <div className="max-w-[1200px] mx-auto flex items-center justify-between px-5 sm:px-10 h-16">
          <a href="/" className="inline-flex items-center gap-2.5" aria-label="FII Guia">
            <img src={isDark ? logoDark : logoLight} alt="FII Guia" className="h-7 w-auto block" />
          </a>
          <nav className="hidden md:flex items-center gap-0.5" aria-label="Navegação principal">
            <a href="/#what"       className="text-[13px] text-muted-foreground px-3 py-2 rounded-lg hover:bg-secondary hover:text-foreground transition-colors">O que é um FII</a>
            <a href="/#context"    className="text-[13px] text-muted-foreground px-3 py-2 rounded-lg hover:bg-secondary hover:text-foreground transition-colors">Dados com contexto</a>
            <a href="/#ai"         className="text-[13px] text-muted-foreground px-3 py-2 rounded-lg hover:bg-secondary hover:text-foreground transition-colors">Resumo do gestor</a>
            <a href="/#principles" className="text-[13px] text-muted-foreground px-3 py-2 rounded-lg hover:bg-secondary hover:text-foreground transition-colors">Princípios</a>
          </nav>
          <button
            onClick={toggle}
            aria-label={isDark ? 'Mudar para tema claro' : 'Mudar para tema escuro'}
            className="inline-flex items-center gap-2 px-3 py-1.5 rounded-full border border-border/60 bg-card text-muted-foreground text-[11px] font-mono tracking-wider hover:text-foreground hover:border-border transition-colors"
          >
            {isDark ? <Sun className="w-[13px] h-[13px]" /> : <Moon className="w-[13px] h-[13px]" />}
            <span>{isDark ? 'tema claro' : 'tema escuro'}</span>
          </button>
        </div>
      </header>

      {/* Search */}
      <SearchBar onSearch={onSearch} initialValue={tickerParam || ''} />

      {/* Error */}
      {error && (
        <div className="max-w-[920px] mx-auto px-5 mt-4">
          <div className="flex items-center gap-2 bg-loss/10 border border-loss/20 rounded-lg px-4 py-2.5">
            <span className="text-loss text-sm font-mono">{error}</span>
          </div>
        </div>
      )}

      {/* Content */}
      <div
        className={`relative max-w-[920px] mx-auto px-4 sm:px-5 py-8 pb-20 transition-all duration-300 ${loading ? 'opacity-30 pointer-events-none' : 'opacity-100'}`}
      >
        {fund && (
          <div className="flex flex-col gap-5">
            <div className="animate-fade-in" style={{ animationDelay: '0ms' }}>
              <FundHeader fund={fund} />
            </div>
            <div className="animate-fade-in" style={{ animationDelay: '60ms' }}>
              <PriceCard fund={fund} />
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-5 animate-fade-in" style={{ animationDelay: '120ms' }}>
              <VolatilityCard fund={fund} />
              <LiquidityCard fund={fund} />
            </div>
            <div className="animate-fade-in" style={{ animationDelay: '160ms' }}>
              <HealthCard fund={fund} />
            </div>
            <div className="animate-fade-in" style={{ animationDelay: '180ms' }}>
              <DividendCard fund={fund} />
            </div>
            <div className="animate-fade-in" style={{ animationDelay: '240ms' }}>
              <SimulatorCard fund={fund} />
            </div>
            <div className="animate-fade-in" style={{ animationDelay: '300ms' }}>
              <AIManagerCard fund={fund} />
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
