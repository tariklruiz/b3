import { useState } from 'react'
import { type FundData, fmtBRL } from '@/lib/fii-helpers'
import { SecLabel } from './PriceCard'

export function SimulatorCard({ fund }: { fund: FundData }) {
  const [investimento, setInvestimento] = useState(10000)

  const cotas = Math.floor(investimento / fund.preco)
  const mensal = fund.div_avg12 != null ? Math.round(cotas * fund.div_avg12) : null
  const anual = mensal != null ? mensal * 12 : null
  const grossup = (fund.dy_anual != null && fund.cdi_anual > 0)
    ? (fund.dy_anual / (1 - 0.20) / fund.cdi_anual * 100).toFixed(0)
    : null

  return (
    <div className="bg-card border border-border rounded-xl p-6 sm:p-8 shadow-sm transition-colors">
      <div className="flex items-center justify-between mb-5 flex-wrap gap-3">
        <SecLabel className="!mb-0 flex-1 min-w-[120px]">Simulador</SecLabel>
        <div className="flex items-center gap-2.5">
          <span className="text-xs text-muted-foreground">investimento</span>
          <div className="flex items-center bg-secondary border border-border rounded-lg h-[34px] px-3 gap-1.5 focus-within:border-primary/40 transition-all">
            <span className="text-xs text-muted-foreground font-mono">R$</span>
            <input
              type="number" min={100} step={1000} value={investimento}
              onChange={(e) => setInvestimento(Math.max(0, parseFloat(e.target.value) || 0))}
              className="w-[90px] bg-transparent border-none outline-none font-mono text-sm text-foreground text-right"
            />
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <SimSurface label="cotas compradas" value={cotas.toLocaleString('pt-BR')} sub={`a ${fmtBRL(fund.preco)}`} />
        <SimSurface
          label={fund.div_avg12 != null ? `renda mensal a ${fmtBRL(fund.div_avg12)}` : 'renda mensal'}
          value={mensal != null ? `R$ ${mensal.toLocaleString('pt-BR')}` : '—'}
          sub="média 12M · por mês" highlight
        />
        <SimSurface label="renda anual" value={anual != null ? `R$ ${anual.toLocaleString('pt-BR')}` : '—'} sub="isento de IR para PF" />
        <SimSurfaceGrossup grossup={grossup} anual={anual} />
      </div>

      <p className="text-[11px] text-muted-foreground/70 font-mono mt-5 pt-4 border-t border-border leading-relaxed">
        simulação pela média de dividendos dos últimos 12 meses. rendimentos passados não garantem resultados futuros.
      </p>
    </div>
  )
}

function SimSurface({ label, value, sub, highlight }: { label: string; value: string; sub: string; highlight?: boolean }) {
  return (
    <div className={`bg-secondary border rounded-lg p-4 transition-colors ${highlight ? 'border-accent/30 hover:border-accent/50' : 'border-border'}`}>
      <p className="text-[10px] text-muted-foreground leading-snug">{label}</p>
      <p className={`text-xl font-bold mt-2 tabular-nums ${highlight ? 'text-accent' : 'text-foreground'}`}>{value}</p>
      <p className="text-[10px] text-muted-foreground/70 font-mono mt-1">{sub}</p>
    </div>
  )
}

function SimSurfaceGrossup({ grossup, anual }: { grossup: string | null; anual: number | null }) {
  return (
    <div className="bg-secondary border border-border rounded-lg p-4 transition-colors">
      <div className="flex items-center gap-1.5">
        <p className="text-[10px] text-muted-foreground">equiv. renda fixa 12M</p>
        <div className="group/gt relative inline-flex">
          <div className="w-4 h-4 rounded-full bg-muted border border-border text-[9px] font-mono text-muted-foreground flex items-center justify-center cursor-help hover:border-primary/40 hover:text-primary transition-colors">?</div>
          <div className="hidden group-hover/gt:block absolute left-0 top-6 z-50 w-[280px] bg-card border border-border rounded-xl p-4 text-xs text-foreground leading-relaxed shadow-xl">
            <p className="font-semibold mb-2">o que é gross-up?</p>
            {anual != null && (
              <p className="text-muted-foreground text-[11px] mb-3">
                Quanto precisaria render uma aplicação tributada (CDB, Tesouro) para entregar os mesmos R$ {anual.toLocaleString('pt-BR')} líquidos que este FII entrega isento de IR no ano.
              </p>
            )}
            <p className="text-[11px] font-semibold mb-2">alíquotas IR — renda fixa:</p>
            <table className="w-full text-[10px] font-mono">
              <tbody>
                {[['até 180 dias', '22,5%'], ['181 a 360 dias', '20,0%'], ['361 a 720 dias', '17,5%'], ['acima de 720 dias', '15,0%']].map(([p, r]) => (
                  <tr key={p}><td className="py-0.5 text-muted-foreground">{p}</td><td className="py-0.5 text-right text-foreground/80">{r}</td></tr>
                ))}
              </tbody>
            </table>
            <p className="text-[10px] text-muted-foreground/70 mt-2">Aqui usamos 20% (181–360 dias).</p>
          </div>
        </div>
      </div>
      <p className="text-xl font-bold text-foreground mt-2 tabular-nums">{grossup != null ? `${grossup}% CDI` : '—'}</p>
      <p className="text-[10px] text-muted-foreground/70 font-mono mt-1">gross-up 20% · 181–360d</p>
    </div>
  )
}
