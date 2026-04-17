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
      <div className="flex gap-2 mt-6 flex-wrap">
        {pills.map(p => (
          <div key={p.key} className="bg-secondary border border-border rounded-lg px-3 py-2 text-center flex-1 min-w-[52px] hover:bg-muted transition-colors">
            <div className="text-[9px] text-muted-foreground font-mono uppercase tracking-wider">{p.label}</div>
            <div className={`text-sm font-mono font-semibold mt-1 ${pctColor(fund[p.key])}`}>{fmtPct(fund[p.key])}</div>
          </div>
        ))}
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
      <div className="hidden group-hover:block absolute left-0 top-7 z-50 w-[280px] bg-card border border-border rounded-xl p-4 text-xs text-foreground leading-relaxed shadow-xl">
        {text}
      </div>
    </div>
  )
}
