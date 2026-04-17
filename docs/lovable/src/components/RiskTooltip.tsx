import { useState } from 'react'
import { type FundData, buildRiskSignals } from '@/lib/fii-helpers'

const scoreColors = ['text-profit', 'text-amber', 'text-loss']
const scoreBg = ['bg-profit', 'bg-amber', 'bg-loss']

export function RiskTooltip({ fund }: { fund: FundData }) {
  const [open, setOpen] = useState(false)
  const signals = buildRiskSignals(fund)

  let totalPts = 0
  signals.forEach(s => { if (s.score !== null) totalPts += s.score })

  const riskColor = totalPts <= 3 ? 'text-profit' : totalPts <= 7 ? 'text-amber' : 'text-loss'
  const riskLabel = totalPts <= 3 ? 'baixo' : totalPts <= 7 ? 'médio' : 'alto'

  return (
    <div className="relative inline-flex items-center">
      <button
        onClick={() => setOpen(!open)}
        className="w-[15px] h-[15px] rounded-full surface-deep border border-hairline border-border text-[9px] font-mono text-muted-foreground/80 flex items-center justify-center hover:text-accent hover:border-accent/40 transition-colors"
        aria-label="Detalhes do cálculo de risco"
      >
        ?
      </button>
      {open && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
          <div className="absolute left-0 top-6 z-50 min-w-[360px] max-w-[460px] surface-elevated border border-hairline border-border rounded-[10px] p-4 shadow-2xl">
            <p className="text-[13px] font-medium text-foreground mb-1.5">como calculamos o risco</p>
            <p className="text-[10px] text-muted-foreground mb-0.5">Cada sinal recebe 0 pts (baixo), 1 pt (médio) ou 2 pts (alto).</p>
            <p className="text-[10px] text-muted-foreground mb-3">Total: 0–3 = baixo · 4–7 = médio · 8+ = alto.</p>

            <table className="w-full text-[10px] font-mono">
              <thead>
                <tr className="text-muted-foreground/70 text-[9px]">
                  <td className="pb-2 border-b border-border pr-2">Sinal</td>
                  <td className="pb-2 border-b border-border text-right pr-2">Valor</td>
                  <td className="pb-2 border-b border-border text-center px-1">Baixo</td>
                  <td className="pb-2 border-b border-border text-center px-1">Médio</td>
                  <td className="pb-2 border-b border-border text-center px-1">Alto</td>
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

            <div className="mt-3 pt-2.5 border-t border-hairline border-border">
              <p className={`text-[12px] font-bold ${riskColor}`}>Pontuação: {totalPts} pts → risco {riskLabel}</p>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
