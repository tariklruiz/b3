import { type FundData, fmtBRL, fmtPctAbs, waffleColor } from '@/lib/fii-helpers'
import { SecLabel } from './PriceCard'

function BenchmarkBadge({ fund }: { fund: FundData }) {
  if (fund.bench_median_mensal == null || fund.dy_anual == null) return null

  const dyMensal = fund.dy_anual / 12
  const diff = dyMensal - fund.bench_median_mensal
  const tol  = fund.bench_median_mensal * 0.03

  if (diff > tol) {
    return (
      <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-mono font-semibold border border-profit/40 bg-profit/10 text-profit">
        acima do benchmark
      </span>
    )
  } else if (diff < -tol) {
    return (
      <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-mono font-semibold border border-amber/40 bg-amber/10 text-amber">
        abaixo do benchmark
      </span>
    )
  } else {
    return (
      <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-mono font-semibold border border-primary/40 bg-primary/10 text-primary">
        igual ao benchmark
      </span>
    )
  }
}

export function DividendCard({ fund }: { fund: FundData }) {
  const pct = Math.round(fund.div_pagos / fund.div_total * 100)

  return (
    <div className="bg-card border border-border rounded-xl shadow-sm transition-colors">
      <div className="flex flex-col md:flex-row items-stretch">
        {/* Left: last dividend + waffle + legend */}
        <div className="flex-1 min-w-0 p-6 sm:p-7">
          <SecLabel>Dividendos — últimos 13 meses</SecLabel>
          <p className="text-[11px] text-muted-foreground mb-1">último dividendo</p>
          <div className="flex items-baseline gap-2">
            <span className="text-2xl font-bold text-foreground tabular-nums">{fmtBRL(fund.div_valor)}</span>
            <span className="text-[11px] text-muted-foreground font-mono">pago em {fund.div_pago_em}</span>
          </div>
          <p className="text-[10px] text-muted-foreground/70 font-mono mt-1">data-base: {fund.div_base}</p>
          <div className="flex items-center gap-3 mt-3 mb-5 flex-wrap">
            <span className="text-sm font-semibold text-foreground">pagou {fund.div_pagos} de {fund.div_total} meses</span>
            <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-mono font-semibold border border-profit/40 bg-profit/10 text-profit">
              <span className="w-1.5 h-1.5 rounded-full bg-current" />
              {pct}%
            </span>
          </div>

          {/* Waffle */}
          <div className="flex gap-[5px] items-start pb-2 overflow-x-auto">
            {fund.div_historico.map((d, i) => {
              const prev = i > 0 ? fund.div_historico[i - 1].v : d.v
              const color = waffleColor(d.v, prev)
              return (
                <div key={i} className="flex flex-col items-center gap-1 group relative min-w-[22px]">
                  <div className="hidden group-hover:block absolute left-7 top-0 bg-card border border-border rounded-lg px-2.5 py-1.5 text-[10px] text-foreground font-mono whitespace-nowrap z-50 shadow-lg">
                    {d.v !== null ? fmtBRL(d.v) : 'não pagou'}
                  </div>
                  <div
                    className="w-[22px] h-[22px] rounded cursor-pointer transition-all duration-150 hover:scale-110 hover:brightness-110"
                    style={{ background: color }}
                  />
                  <span className="text-[7px] text-muted-foreground/60 font-mono whitespace-nowrap mt-1.5 h-4" style={{ transform: 'rotate(-45deg)', transformOrigin: 'top center' }}>
                    {d.m}
                  </span>
                </div>
              )
            })}
          </div>

          {/* Legend */}
          <div className="mt-4">
            <div className="text-[9px] font-semibold text-muted-foreground/70 uppercase tracking-wider font-mono mb-2">Legenda de cores</div>
            <div className="grid grid-cols-2 gap-x-6 gap-y-1.5 text-[10px] text-muted-foreground font-mono w-fit">
              <span className="flex items-center gap-1.5"><div className="w-2.5 h-2.5 rounded-sm bg-info-blue flex-shrink-0" />igual ao mês anterior</span>
              <span className="flex items-center gap-1.5"><div className="w-2.5 h-2.5 rounded-sm bg-profit flex-shrink-0" />maior que o mês anterior</span>
              <span className="flex items-center gap-1.5"><div className="w-2.5 h-2.5 rounded-sm bg-amber flex-shrink-0" />menor que o mês anterior</span>
              <span className="flex items-center gap-1.5"><div className="w-2.5 h-2.5 rounded-sm bg-loss flex-shrink-0" />sem pagamento no mês</span>
            </div>
          </div>
        </div>

        {/* Vertical divider (desktop only) */}
        <div className="hidden md:block w-px bg-border self-stretch flex-shrink-0" />
        <div className="md:hidden h-px bg-border" />

        {/* Right: Dividend Yield + benchmark */}
        <div className="md:flex-shrink-0 p-6 sm:p-7">
          <SecLabel>Dividend Yield</SecLabel>
          <div className="flex items-baseline gap-5 mt-2">
            <div>
              <p className="text-[10px] text-muted-foreground font-mono mb-1">mensal</p>
              <span className="text-2xl font-bold text-foreground tabular-nums">{fmtPctAbs(fund.dy_anual / 12)}</span>
            </div>
            <div className="w-px h-10 bg-border self-center" />
            <div>
              <p className="text-[10px] text-muted-foreground font-mono mb-1">anualizado</p>
              <span className="text-2xl font-bold text-accent tabular-nums">{fmtPctAbs(fund.dy_anual)}</span>
            </div>
          </div>
          <p className="text-[10px] text-muted-foreground/70 font-mono mt-2">soma dos últimos 12 dividendos / preço atual</p>
          {fund.bench_hint && <p className="text-[10px] text-muted-foreground font-mono mt-1">{fund.bench_hint}</p>}
          <div className="mt-2">
            <BenchmarkBadge fund={fund} />
          </div>
        </div>
      </div>
    </div>
  )
}
