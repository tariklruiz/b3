import { useState, useRef, useEffect } from 'react'
import { type FundData, buildRiskSignals } from '@/lib/fii-helpers'

const scoreColors = ['text-profit', 'text-amber', 'text-loss']
const scoreBg = ['bg-profit', 'bg-amber', 'bg-loss']

export function RiskTooltip({ fund }: { fund: FundData }) {
  const [open, setOpen] = useState(false)
  const [pos, setPos] = useState({ top: 0, left: 0 })
  const btnRef = useRef<HTMLButtonElement>(null)
  const signals = buildRiskSignals(fund)

  let totalPts = 0, nLow = 0, nMed = 0, nHi = 0
  signals.forEach(s => {
    if (s.score !== null) {
      totalPts += s.score
      if (s.score === 0) nLow++
      else if (s.score === 1) nMed++
      else nHi++
    }
  })

  const riskColor = totalPts <= 3 ? 'text-profit' : totalPts <= 7 ? 'text-amber' : 'text-loss'
  const riskLabel = totalPts <= 3 ? 'baixo' : totalPts <= 7 ? 'médio' : 'alto'

  function handleOpen() {
    if (btnRef.current) {
      const rect = btnRef.current.getBoundingClientRect()
      const tooltipWidth = 420
      // Position below the button, right-aligned to button's right edge
      let left = rect.left
      // Clamp to viewport
      if (left < 10) left = 10
      if (left + tooltipWidth > window.innerWidth - 10) left = window.innerWidth - tooltipWidth - 10
      setPos({ top: rect.bottom + 8, left })
    }
    setOpen(o => !o)
  }

  useEffect(() => {
    if (!open) return
    const close = () => setOpen(false)
    window.addEventListener('scroll', close, true)
    window.addEventListener('resize', close)
    return () => {
      window.removeEventListener('scroll', close, true)
      window.removeEventListener('resize', close)
    }
  }, [open])

  return (
    <>
      <button
        ref={btnRef}
        onClick={handleOpen}
        className="w-[15px] h-[15px] rounded-full bg-secondary border border-border text-[9px] font-mono text-muted-foreground/80 flex items-center justify-center hover:text-accent hover:border-accent/40 transition-colors"
        aria-label="Detalhes do cálculo de risco"
      >
        ?
      </button>

      {open && (
        <>
          <div className="fixed inset-0 z-[99]" onClick={() => setOpen(false)} />
          <div
            className="fixed z-[100] min-w-[380px] max-w-[460px] bg-card border border-border rounded-[10px] p-4 shadow-2xl"
            style={{ top: pos.top, left: pos.left }}
          >
            <p className="text-[13px] font-medium text-foreground mb-1.5">como calculamos o risco</p>
            <p className="text-[10px] text-muted-foreground mb-0.5">Cada sinal recebe 0 pts (baixo), 1 pt (médio) ou 2 pts (alto).</p>
            <p className="text-[10px] text-muted-foreground mb-3">Total: 0–3 = baixo · 4–7 = médio · 8+ = alto.</p>

            <table className="w-full text-[10px] font-mono">
              <thead>
                <tr className="text-muted-foreground/70 text-[9px]">
                  <td className="pb-2 border-b border-border pr-2 whitespace-nowrap">Sinal</td>
                  <td className="pb-2 border-b border-border text-right pr-2 whitespace-nowrap">Valor</td>
                  <td className="pb-2 border-b border-border text-center px-1 whitespace-nowrap">Baixo (0)</td>
                  <td className="pb-2 border-b border-border text-center px-1 whitespace-nowrap">Médio (1)</td>
                  <td className="pb-2 border-b border-border text-center px-1 whitespace-nowrap">Alto (2)</td>
                </tr>
              </thead>
              <tbody>
                {signals.map((s, i) => {
                  const valStr = s.val != null ? s.fmt(s.val) : '—'
                  const nameColor = s.score !== null && s.score > 0 ? scoreColors[s.score] : 'text-muted-foreground'
                  const levels = [s.low, s.med, s.hi]
                  return (
                    <tr key={i}>
                      <td className={`py-1.5 pr-2 whitespace-nowrap ${nameColor}`}>{s.name}</td>
                      <td className="py-1.5 pr-2 text-right text-muted-foreground whitespace-nowrap">{valStr}</td>
                      {levels.map((label, li) => {
                        const active = s.score !== null && s.score === li
                        return (
                          <td key={li} className="py-1.5 px-1 text-center whitespace-nowrap">
                            <span className={`inline-block px-1.5 py-0.5 rounded text-[9px] ${active ? `${scoreBg[li]} text-white font-bold` : 'text-muted-foreground/40'}`}>
                              {label}
                            </span>
                          </td>
                        )
                      })}
                    </tr>
                  )
                })}
              </tbody>
            </table>

            <div className="mt-3 pt-2.5 border-t border-border">
              <p className={`text-[12px] font-bold ${riskColor}`}>
                Pontuação: {totalPts} pts → risco {riskLabel}
              </p>
              <p className="text-[10px] text-muted-foreground/70 font-mono mt-1">
                {nLow} sinal(is) baixo · {nMed} médio · {nHi} alto
              </p>
            </div>
            <p className="text-[9px] text-muted-foreground/50 font-mono mt-2">
              Vacância e risco de default em breve.
            </p>
          </div>
        </>
      )}
    </>
  )
}
