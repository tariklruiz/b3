import { type FundData, fmtBRL } from '@/lib/fii-helpers'
import { SecLabel } from './PriceCard'

const tomMap: Record<string, string> = {
  conservador: 'border-loss/40 bg-loss/10 text-loss',
  neutro:      'border-border bg-secondary text-muted-foreground',
  otimista:    'border-profit/40 bg-profit/10 text-profit',
}

const nivelMap: Record<string, string> = {
  critico:     'border-loss/40 bg-loss/10 text-loss',
  atencao:     'border-amber/40 bg-amber/10 text-amber',
  provisionado:'border-border bg-secondary text-muted-foreground',
}

const statusIcon: Record<string, string> = {
  sem_alteracao:     '→',
  piora:             '↓',
  resolucao_parcial: '↑',
  resolvido:         '✓',
}

export function AIManagerCard({ fund }: { fund: FundData }) {
  const g = fund.gestor

  return (
    <div className="bg-card border border-border rounded-xl p-6 sm:p-8 shadow-sm transition-colors">
      <div className="flex items-center gap-2.5 mb-4 flex-wrap">
        <SecLabel className="!mb-0">resumo do gestor</SecLabel>
        <span className="px-2 py-0.5 rounded-full text-[10px] font-mono font-semibold border border-primary/30 bg-primary/10 text-primary">IA</span>
        {g && (
          <>
            <span className="text-[10px] text-muted-foreground/70 font-mono">ref. {g.competencia}</span>
            {g.tom_gestor && (
              <span className={`px-2 py-0.5 rounded-full text-[10px] font-mono font-semibold border ${tomMap[g.tom_gestor] ?? tomMap.neutro}`}>
                {g.tom_gestor}
              </span>
            )}
          </>
        )}
        {!g && (
          <span className="px-2 py-0.5 rounded-full text-[10px] font-mono font-medium border border-border bg-secondary text-muted-foreground">
            em construção
          </span>
        )}
      </div>

      {g ? (
        <div className="space-y-5">
          {/* Resumo + Mudanças — 2 columns on desktop */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
            <div>
              <p className="text-[10px] text-muted-foreground font-mono mb-2 uppercase tracking-wider">resumo</p>
              <p className="text-sm text-foreground/90 leading-relaxed">{g.resumo || '—'}</p>
            </div>
            <div>
              <p className="text-[10px] text-muted-foreground font-mono mb-2 uppercase tracking-wider">mudanças no portfólio</p>
              <p className="text-sm text-foreground/90 leading-relaxed">{g.mudancas_portfolio || '—'}</p>
            </div>
          </div>

          {/* CRIs em observação */}
          {g.cris_em_observacao && g.cris_em_observacao.length > 0 && (
            <div>
              <p className="text-[10px] text-muted-foreground font-mono mb-2 uppercase tracking-wider">CRIs em observação</p>
              <div className="flex flex-wrap gap-2">
                {g.cris_em_observacao.map((c, i) => (
                  <span key={i} className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-mono font-semibold border ${nivelMap[c.nivel] ?? nivelMap.provisionado}`}>
                    {statusIcon[c.status] || '→'} {c.nome}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Histórico de caixa */}
          {g.contexto_meses && g.contexto_meses.length > 0 && (
            <div>
              <p className="text-[10px] text-muted-foreground font-mono mb-2 uppercase tracking-wider">histórico de caixa</p>
              <div className="overflow-x-auto">
                <table className="text-[11px] font-mono border-collapse">
                  <thead>
                    <tr className="text-muted-foreground/70 text-[9px]">
                      <td className="pb-2 pr-6 border-b border-border">Mês</td>
                      <td className="pb-2 pr-6 border-b border-border text-right">Resultado</td>
                      <td className="pb-2 pr-6 border-b border-border text-right">Distribuição</td>
                      <td className="pb-2 border-b border-border text-right">Reserva</td>
                    </tr>
                  </thead>
                  <tbody>
                    {g.contexto_meses.map((m, i) => (
                      <tr key={i} className="border-t border-border/50">
                        <td className="py-1.5 pr-6 text-muted-foreground">{m.mes}</td>
                        <td className="py-1.5 pr-6 text-right">{m.resultado != null ? fmtBRL(m.resultado) : '—'}</td>
                        <td className="py-1.5 pr-6 text-right">{m.distribuicao != null ? fmtBRL(m.distribuicao) : '—'}</td>
                        <td className="py-1.5 text-right text-muted-foreground">{m.reserva_brl != null ? `R$ ${(m.reserva_brl / 1e6).toFixed(2)}M` : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Alerta de dados */}
          {g.alertas_dados && (
            <div className="flex items-start gap-2.5 bg-amber/10 border border-amber/30 rounded-xl px-4 py-3">
              <span className="text-amber mt-0.5 flex-shrink-0">⚠</span>
              <p className="text-[11px] text-amber font-mono leading-relaxed">{g.alertas_dados}</p>
            </div>
          )}

          <p className="text-[10px] text-muted-foreground/50 font-mono pt-2 border-t border-border">
            processado em {g.processado_em} · modelo claude-sonnet
          </p>
        </div>
      ) : (
        <div className="flex flex-col items-center justify-center gap-3 py-10 bg-secondary border border-dashed border-border rounded-xl">
          <span className="text-2xl opacity-30">⚙</span>
          <p className="text-xs text-muted-foreground font-mono tracking-wide text-center px-4">
            pipeline de leitura dos relatórios gerenciais em construção
          </p>
          <p className="text-[10px] text-muted-foreground/60 font-mono">
            disponível após processamento dos PDFs via IA
          </p>
        </div>
      )}
    </div>
  )
}
