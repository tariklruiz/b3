import { type FundData, riscoBadge, fmtNome, fmtMes } from '@/lib/fii-helpers'
import { RiskTooltip } from './RiskTooltip'

const badgeColorMap: Record<string, string> = {
  profit: 'border-profit/40 bg-profit/10 text-profit',
  amber: 'border-amber/40 bg-amber/10 text-amber',
  loss: 'border-loss/40 bg-loss/10 text-loss',
}

export function FundHeader({ fund }: { fund: FundData }) {
  const [color, label] = riscoBadge(fund.risco)
  const badgeClass = badgeColorMap[color] || badgeColorMap.profit

  const subParts: string[] = []
  if (fund.administrador) subParts.push(fmtNome(fund.administrador) ?? '')
  if (fund.cotistas) subParts.push(fund.cotistas.toLocaleString('pt-BR') + ' cotistas')
  if (fund.competencia) subParts.push('ref. ' + fmtMes(fund.competencia))

  return (
    <section className="flex flex-col sm:flex-row sm:items-end justify-between gap-4 mb-2">
      <div className="flex flex-col gap-2">
        <div className="flex items-center gap-4 flex-wrap">
          <h1 className="text-4xl sm:text-5xl font-bold text-foreground tracking-tight tabular-nums leading-none">
            {fund.ticker}
          </h1>
          <span className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-[11px] font-mono font-semibold border uppercase tracking-wide ${badgeClass}`}>
            <span className="w-1.5 h-1.5 rounded-full bg-current" />
            {label}
          </span>
          <RiskTooltip fund={fund} />
        </div>
        <p className="text-foreground/85 text-sm font-semibold">{fund.nome}</p>
        <p className="text-muted-foreground text-xs font-mono">
          {fund.segmento}
          {subParts.length > 0 && <><br /><span className="text-muted-foreground/70">{subParts.join(' · ')}</span></>}
        </p>
      </div>
      <p className="text-[10px] text-muted-foreground/70 font-mono shrink-0">{fund.atualizado}</p>
    </section>
  )
}
