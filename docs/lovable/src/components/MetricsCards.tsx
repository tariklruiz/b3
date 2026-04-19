import { type FundData, fmtPctAbs, volBadge, liqBadge, fmtK } from '@/lib/fii-helpers'
import { SecLabel, Tip } from './PriceCard'

const badgeStyles: Record<string, string> = {
  profit: 'border-profit/40 bg-profit/10 text-profit',
  amber: 'border-amber/40 bg-amber/10 text-amber',
  loss: 'border-loss/40 bg-loss/10 text-loss',
}

function MetricCard({ title, tooltip, value, badgeColor, badgeLabel, pills }: {
  title: string; tooltip: string; value: string; badgeColor: string; badgeLabel: string
  pills: { label: string; val: string }[]
}) {
  return (
    <div className="bg-card border border-border rounded-xl p-5 sm:p-6 shadow-sm hover:shadow-md transition-all duration-200">
      <div className="flex items-center gap-2 mb-3">
        <div className="flex items-center gap-1.5 flex-1 min-w-0">
          <span className="text-[10px] text-primary uppercase tracking-[0.15em] font-semibold whitespace-nowrap">{title}</span>
          <Tip text={tooltip} />
          <div className="flex-1 h-px bg-gradient-to-r from-border to-transparent" />
        </div>
      </div>
      <div className="flex items-center gap-3 mt-3 mb-4">
        <span className="text-3xl font-bold text-foreground tabular-nums tracking-tight">{value}</span>
        <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-mono font-semibold border ${badgeStyles[badgeColor]}`}>
          <span className="w-1.5 h-1.5 rounded-full bg-current" />
          {badgeLabel}
        </span>
      </div>
      <div className="flex gap-2">
        {pills.map(p => (
          <div key={p.label} className="bg-secondary border border-border rounded-lg px-2.5 py-1.5 text-center flex-1 hover:bg-muted transition-colors">
            <div className="text-[8px] text-muted-foreground font-mono uppercase tracking-wider">{p.label}</div>
            <div className="text-xs text-foreground/80 font-mono font-medium mt-0.5">{p.val}</div>
          </div>
        ))}
      </div>
    </div>
  )
}

export function VolatilityCard({ fund }: { fund: FundData }) {
  const [color, label] = volBadge(fund.vol_90d)
  return (
    <MetricCard
      title="Índice de Volatilidade" tooltip="Desvio padrão dos retornos diários × √252. Baixo <15% · Médio 15–25% · Alto >25%"
      value={fmtPctAbs(fund.vol_90d)} badgeColor={color} badgeLabel={label}
      pills={[
        { label: '7d', val: fmtPctAbs(fund.vol_7d) }, { label: '30d', val: fmtPctAbs(fund.vol_30d) },
        { label: '90d', val: fmtPctAbs(fund.vol_90d) }, { label: '12M', val: fmtPctAbs(fund.vol_12m) },
      ]}
    />
  )
}

export function LiquidityCard({ fund }: { fund: FundData }) {
  const [color, label] = liqBadge(fund.liq_30d)
  return (
    <MetricCard
      title="Liquidez — Transações" tooltip="Número de negócios por dia. Baixo <5k · Médio 5k–20k · Alto >20k"
      value={fmtK(Math.round(fund.liq_30d))} badgeColor={color} badgeLabel={label}
      pills={[
        { label: '7d', val: fmtK(fund.liq_7d) }, { label: '30d', val: fmtK(fund.liq_30d) },
        { label: '90d', val: fmtK(fund.liq_90d) }, { label: '12M', val: fmtK(fund.liq_12m) },
      ]}
    />
  )
}
