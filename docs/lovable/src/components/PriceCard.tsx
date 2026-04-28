import { type FundData, fmtPct, pctColor } from '@/lib/fii-helpers'

const pills = [
  { key: 'preco_d1'   as const, label: 'D-1'  },
  { key: 'preco_d7'   as const, label: 'D-7'  },
  { key: 'preco_d30'  as const, label: 'D-30' },
  { key: 'preco_d90'  as const, label: 'D-90' },
  { key: 'preco_d180' as const, label: 'D-180'},
  { key: 'preco_12m'  as const, label: '12M'  },
]

export function PriceCard({ fund }: { fund: FundData }) {
  const pvpDiscount = fund.pvp != null ? ((1 - fund.pvp) * 100).toFixed(0) : null

  // ----- Dynamic P/VP range -----
  // Standard range is [0.70, 1.30]. Expand only when the value falls outside:
  //   pvp < 0.70 → min becomes pvp * 0.8
  //   pvp > 1.30 → max becomes pvp * 1.2
  // Color: yellow (amber) if pvp < 0.8 or > 1.2, otherwise green (profit)
  const pvp = fund.pvp
  let pvpMin = 0.70
  let pvpMax = 1.30
  if (pvp != null) {
    if (pvp < 0.70) pvpMin = pvp * 0.8
    if (pvp > 1.30) pvpMax = pvp * 1.2
  }
  const pvpRange = pvpMax - pvpMin
  const pvpFillWidth = pvp != null
    ? Math.max(0, Math.min(100, ((pvp - pvpMin) / pvpRange) * 100)).toFixed(1)
    : '0'
  // 1.00 reference tick — clamp to bar edge if outside the range
  const justoTickPct = Math.max(0, Math.min(100, ((1 - pvpMin) / pvpRange) * 100)).toFixed(1)
  const pvpAlert = pvp != null && (pvp < 0.8 || pvp > 1.2)
  // Tailwind doesn't dynamically generate gradients from variables, so we
  // pick the gradient class up front (both themes use the same green/yellow).
  const pvpFillClass = pvpAlert
    ? 'bg-gradient-to-r from-amber to-amber'
    : 'bg-gradient-to-r from-profit to-profit'

  // Brazilian decimal formatting (1.20 → "1,20")
  const fmtBr = (n: number) => n.toFixed(2).replace('.', ',')

  return (
    <div className="relative bg-card border border-border rounded-xl p-6 sm:p-8 shadow-sm transition-colors overflow-hidden">
      {/* Top accent line */}
      <div className="absolute top-0 left-0 right-0 h-[2px] bg-gradient-to-r from-primary via-accent to-primary" />

      <SecLabel>Cotação</SecLabel>

      <div className="flex items-baseline gap-3">
        <span className="text-4xl sm:text-[44px] font-bold text-foreground tabular-nums tracking-tight leading-none">
          R$ {fund.preco.toFixed(2).replace('.', ',')}
        </span>
        <span className="text-xs font-mono text-muted-foreground">{fund.preco_data}</span>
      </div>

      {/* Pills */}
      <div className="grid grid-cols-3 sm:grid-cols-6 gap-2 mt-6">
        {pills.map(p => (
          <div key={p.key} className="bg-secondary border border-border rounded-lg px-3 py-2 text-center hover:bg-muted transition-colors">
            <div className="text-[9px] text-muted-foreground font-mono uppercase tracking-wider">{p.label}</div>
            <div className={`text-sm font-mono font-semibold mt-1 ${pctColor(fund[p.key])}`}>{fmtPct(fund[p.key])}</div>
          </div>
        ))}
      </div>

      {/* P/VP */}
      <div className="mt-7 pt-6 border-t border-border">
        <div className="flex items-center gap-2 mb-2">
          <SecLabel className="!mb-0">P/VP</SecLabel>
          <Tip text="Preço dividido pelo valor patrimonial da cota. Abaixo de 1,0 = comprando com desconto. Analise com a saúde: desconto em fundo saudável pode ser oportunidade; em fundo com alerta pode ser merecido." />
        </div>
        <div className="flex items-baseline gap-3 mt-1">
          <span className="text-4xl sm:text-[44px] font-bold text-foreground tabular-nums tracking-tight leading-none">
            {fund.pvp != null ? fund.pvp.toFixed(2) : '—'}
          </span>
          {fund.pvp != null && (
            <span className="text-xs text-muted-foreground">
              {fund.pvp < 1 ? `desconto de ${pvpDiscount}%` : `prêmio de ${Math.abs(Number(pvpDiscount))}%`} sobre o valor patrimonial
            </span>
          )}
        </div>
        <div className="relative h-2.5 w-full bg-secondary rounded-full overflow-hidden mt-4">
          <div
            className={`h-full rounded-full transition-all duration-700 ease-out ${pvpFillClass}`}
            style={{ width: `${pvpFillWidth}%` }}
          />
          {/* 1,00 reference tick — clamped to bar edge when range excludes it */}
          <div
            className="absolute top-0 w-[2px] h-full bg-foreground/40 rounded-full"
            style={{ left: `calc(${justoTickPct}% - 1px)` }}
            aria-hidden="true"
          />
        </div>
        <div className="flex justify-between mt-2 text-[9px] text-muted-foreground font-mono">
          <span>{fmtBr(pvpMin)} — desconto</span>
          <span className="text-foreground/60 font-semibold">1,00 = justo</span>
          <span>{fmtBr(pvpMax)} — prêmio</span>
        </div>
      </div>
    </div>
  )
}

export function SecLabel({ children, className = '' }: { children: React.ReactNode; className?: string }) {
  return (
    <div className={`flex items-center gap-2 mb-3 ${className}`}>
      <span className="text-[10px] text-primary uppercase tracking-[0.15em] font-semibold">{children}</span>
      <div className="flex-1 h-px bg-gradient-to-r from-border to-transparent" />
    </div>
  )
}

export function Tip({ text }: { text: string }) {
  return (
    <div className="group relative inline-flex">
      <div className="w-4 h-4 rounded-full bg-secondary border border-border text-[9px] font-mono text-muted-foreground flex items-center justify-center cursor-help hover:border-primary/40 hover:text-primary transition-colors">?</div>
      <div className="hidden group-hover:block absolute left-0 top-6 z-[100] w-[280px] bg-card border border-border rounded-xl p-4 text-xs text-foreground leading-relaxed shadow-xl">
        {text}
      </div>
    </div>
  )
}
