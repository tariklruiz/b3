import { useState, useEffect, useCallback } from 'react'
import { Sun, Moon } from 'lucide-react'
import { SearchBar } from '@/components/SearchBar'
import { FundHeader } from '@/components/FundHeader'
import { PriceCard } from '@/components/PriceCard'
import { VolatilityCard, LiquidityCard } from '@/components/MetricsCards'
import { DividendCard } from '@/components/DividendCard'
import { SimulatorCard } from '@/components/SimulatorCard'
import { AIManagerCard } from '@/components/AIManagerCard'
import { type FundData, buildFundData } from '@/lib/fii-helpers'
import { useTheme } from '@/hooks/use-theme'
import logoLight from '@/assets/logo-light.svg'
import logoDark from '@/assets/logo-dark.svg'

const API_BASE = 'https://fii-prices.up.railway.app'

export default function Index() {
  const [fund, setFund] = useState<FundData | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const { isDark, toggle } = useTheme()

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
      const informe  = informeRes.ok  ? await informeRes.json()  : null
      const benchData = benchRes.ok   ? await benchRes.json()    : null
      const cdiData  = cdiRes.ok      ? await cdiRes.json()      : null
      const gestorData = gestorRes.ok ? await gestorRes.json()   : null

      const f = buildFundData(ticker, preco, div, informe, benchData, cdiData, gestorData)
      setFund(f)

      document.title = `${ticker} — FII Guia`
      window.location.hash = `?ticker=${ticker}`
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    const hash = window.location.hash.replace('#', '')
    const params = new URLSearchParams(hash)
    const initialTicker = params.get('ticker')
    if (initialTicker) loadFund(initialTicker)
  }, [loadFund])

  return (
    <div className="min-h-screen bg-background transition-colors duration-300">
      {/* Top Bar */}
      <header className="bg-card/90 backdrop-blur-xl border-b border-border px-5 sm:px-7 h-[56px] flex items-center justify-between sticky top-0 z-50 transition-colors">
        <a href="?" className="flex items-center group">
          <img
            src={isDark ? logoDark : logoLight}
            alt="FII Guia"
            className="h-9 w-auto block"
          />
        </a>
        <button
          onClick={toggle}
          aria-label={isDark ? 'Mudar para tema claro' : 'Mudar para tema escuro'}
          className="flex items-center gap-2 px-3 py-1.5 rounded-full border border-border bg-secondary text-secondary-foreground text-xs font-mono hover:bg-muted transition-colors"
        >
          {isDark ? <Sun className="w-3.5 h-3.5" /> : <Moon className="w-3.5 h-3.5" />}
          {isDark ? 'tema claro' : 'tema escuro'}
        </button>
      </header>

      {/* Search */}
      <SearchBar onSearch={loadFund} initialValue={fund?.ticker || ''} />

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
