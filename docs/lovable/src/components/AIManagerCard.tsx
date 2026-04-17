import { type FundData } from '@/lib/fii-helpers'
import { SecLabel } from './PriceCard'

export function AIManagerCard({ fund }: { fund: FundData }) {
  return (
    <div className="bg-card border border-border rounded-xl p-6 sm:p-8 shadow-sm transition-colors">
      <div className="flex items-center gap-2.5 mb-4 flex-wrap">
        <SecLabel className="!mb-0">resumo do gestor</SecLabel>
        <span className="px-2 py-0.5 rounded-full text-[10px] font-mono font-semibold border border-primary/30 bg-primary/10 text-primary">IA</span>
        <span className="px-2 py-0.5 rounded-full text-[10px] font-mono font-medium border border-border bg-secondary text-muted-foreground">em construção</span>
      </div>

      {fund.gestor_resumo ? (
        <>
          <p className="text-xs text-muted-foreground font-mono mb-3">tom detectado: {fund.gestor_tom}</p>
          <p className="text-sm text-foreground/90 leading-relaxed">{fund.gestor_resumo}</p>
          <p className="text-[10px] text-muted-foreground/70 font-mono mt-4">relatório gerencial {fund.gestor_mes} · processado {fund.atualizado}</p>
        </>
      ) : (
        <div className="flex flex-col items-center justify-center gap-3 py-10 bg-secondary border border-dashed border-border rounded-xl">
          <span className="text-2xl opacity-30">⚙</span>
          <p className="text-xs text-muted-foreground font-mono tracking-wide text-center px-4">pipeline de leitura dos relatórios gerenciais em construção</p>
          <p className="text-[10px] text-muted-foreground/60 font-mono">disponível após processamento dos PDFs via IA</p>
        </div>
      )}
    </div>
  )
}
