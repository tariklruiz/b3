import { type FundData, type HealthTier, type HealthData } from '@/lib/fii-helpers'
import { InfoTooltip } from './InfoTooltip'

// ============================================================================
// Tier styling — drives dots, pills, text colors
// ============================================================================
const tierStyles: Record<HealthTier, {
  dot: string
  pillBg: string
  pillText: string
  pillBorder: string
}> = {
  saudavel: {
    dot:        'bg-profit',
    pillBg:     'bg-profit/10',
    pillText:   'text-profit',
    pillBorder: 'border-profit/20',
  },
  atencao: {
    dot:        'bg-amber',
    pillBg:     'bg-amber/10',
    pillText:   'text-amber',
    pillBorder: 'border-amber/20',
  },
  risco: {
    dot:        'bg-loss',
    pillBg:     'bg-loss/10',
    pillText:   'text-loss',
    pillBorder: 'border-loss/20',
  },
}

// ============================================================================
// Formatting helpers (local — specific to this card's Brazilian number style)
// ============================================================================
function fmtPct(v: number | null): string {
  if (v == null) return '—'
  return Math.round(v * 100) + '%'
}

function fmtMeses(v: number | null): string {
  if (v == null) return '—'
  // Brazilian decimal: 1.2 → "1,2"
  return v.toFixed(1).replace('.', ',') + ' meses'
}

function fmtCompetencia(isoDate: string | null): string {
  if (!isoDate) return ''
  const [y, m] = isoDate.split('-')
  return `${m}/${y}`
}

// ============================================================================
// Composition chart colors (categorical, not tied to score)
// ============================================================================
interface CompCategory { key: string; label: string; color: string }
const compCategories: CompCategory[] = [
  { key: 'cri_cra_pct',           label: 'CRI/CRA',            color: 'hsl(var(--info-blue))' },
  { key: 'titulos_privados_pct',  label: 'Títulos privados',   color: 'hsl(var(--accent))'    },
  { key: 'fundos_renda_fixa_pct', label: 'Fundos renda fixa',  color: 'hsl(var(--amber))'     },
  { key: 'imoveis_renda_pct',     label: 'Imóveis de renda',   color: 'hsl(var(--profit))'    },
  { key: 'outros_pct',            label: 'Outros',             color: 'hsl(var(--muted-foreground) / 0.5)' },
]

// ============================================================================
// Component
// ============================================================================
export function HealthCard({ fund }: { fund: FundData }) {
  const h = fund.health

  // -- Fallback state when no health data --
  if (!h) {
    return (
      <div className="bg-card border border-border/60 rounded-[10px] p-5 sm:p-6">
        <div className="flex items-center gap-2 mb-1">
          <h3 className="text-[15px] font-medium text-foreground">saúde do fundo</h3>
          <InfoTooltip label="Mais informações sobre saúde do fundo" title="saúde do fundo">
            soma dos pontos de alavancagem e cobertura de dividendos (0 a 2 pontos cada). 4 pts = saudável · 2-3 pts = atenção · 0-1 pts = risco. não é recomendação de investimento.
          </InfoTooltip>
        </div>
        <p className="text-[12px] text-muted-foreground">índice indisponível para este fundo</p>
      </div>
    )
  }

  const tier = tierStyles[h.tier]
  const alv = h.components.alavancagem
  const cob = h.components.cobertura_dividendos
  const tierAlv = tierStyles[alv.tier]
  const tierCob = tierStyles[cob.tier]
  const compet = fmtCompetencia(fund.competencia)

  const isFallbackCov = cob.method === '3m_fallback'
  const isPassThrough = cob.method === 'pass_through'
  const covValueUnavailable = cob.value == null

  return (
    <div className="bg-card border border-border/60 rounded-[10px] p-5 sm:p-6">
      {/* 1. HEADER ROW */}
      <div className="flex items-start justify-between gap-3 mb-5">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="text-[15px] font-medium text-foreground">saúde do fundo</h3>
            <InfoTooltip label="Mais informações sobre saúde do fundo" title="saúde do fundo">
              soma dos pontos de alavancagem e cobertura de dividendos (0 a 2 pontos cada). 4 pts = saudável · 2-3 pts = atenção · 0-1 pts = risco. não é recomendação de investimento.
            </InfoTooltip>
          </div>
          <p className="text-[11px] text-muted-foreground/80 font-mono mt-0.5">
            índice FII Guia{compet && ` · competência ${compet}`}
          </p>
        </div>

        <div className={`inline-flex items-center gap-2 px-2.5 py-1 rounded-full border ${tier.pillBg} ${tier.pillBorder} flex-shrink-0`}>
          <span className={`w-1.5 h-1.5 rounded-full ${tier.dot}`} aria-hidden="true" />
          <span className={`text-[12px] font-medium ${tier.pillText}`}>
            <span className="sr-only">{h.tier_label}, </span>
            {h.tier_label}
          </span>
          <span className="text-[11px] text-muted-foreground font-mono tabular-nums">
            <span className="sr-only">{h.score} de {h.max_score} pontos</span>
            <span aria-hidden="true">{h.score} / {h.max_score}</span>
          </span>
        </div>
      </div>

      {/* 2. METRIC CARDS */}
      <div className="grid grid-cols-1 sm:grid-cols-[repeat(auto-fit,minmax(200px,1fr))] gap-3 mb-5">
        {/* Alavancagem */}
        <div className="border border-border/50 rounded-[8px] p-3.5">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-[12px] text-muted-foreground">alavancagem</span>
            <InfoTooltip label="Mais informações sobre alavancagem" title="alavancagem">
              passivo total dividido pelo ativo total do fundo. mede o quanto do balanço é financiado por dívida. abaixo de 15% = conservador · 15-30% = moderado · acima de 30% = elevado.
            </InfoTooltip>
          </div>
          <div className="text-[22px] font-medium text-foreground tabular-nums leading-none">
            {fmtPct(alv.value)}
          </div>
          <div className="flex items-center gap-1.5 mt-2">
            <span className={`w-1.5 h-1.5 rounded-full ${tierAlv.dot}`} aria-hidden="true" />
            <span className={`text-[12px] ${tierAlv.pillText}`}>{alv.label}</span>
          </div>
        </div>

        {/* Cobertura de dividendos */}
        <div className={`border border-border/50 rounded-[8px] p-3.5 ${covValueUnavailable ? 'opacity-60' : ''}`}>
          <div className="flex items-center gap-2 mb-2 flex-wrap">
            <span className="text-[12px] text-muted-foreground">cobertura de dividendos</span>
            <InfoTooltip label="Mais informações sobre cobertura de dividendos" title="cobertura de dividendos">
              quantos meses o fundo conseguiria pagar dividendos usando apenas a reserva já acumulada, sem depender de novos recebimentos. calculado a partir do informe mensal da CVM e da média de distribuição dos últimos 12 meses. valores maiores indicam mais previsibilidade para os próximos pagamentos.
            </InfoTooltip>
            {isPassThrough && (
              <span className="text-[9px] font-mono uppercase tracking-wider px-1.5 py-0.5 rounded bg-secondary text-muted-foreground border border-border">
                pass-through
              </span>
            )}
            {isFallbackCov && (
              <span className="text-[9px] font-mono uppercase tracking-wider px-1.5 py-0.5 rounded bg-amber/10 text-amber border border-amber/20">
                ⚠ histórico parcial
              </span>
            )}
          </div>
          <div className="text-[22px] font-medium text-foreground tabular-nums leading-none">
            {isPassThrough ? '—' : (covValueUnavailable ? '—' : fmtMeses(cob.value))}
          </div>
          <div className="flex items-center gap-1.5 mt-2">
            <span className={`w-1.5 h-1.5 rounded-full ${tierCob.dot}`} aria-hidden="true" />
            <span className={`text-[12px] ${tierCob.pillText}`}>
              {covValueUnavailable && !isPassThrough ? 'dado indisponível' : cob.label}
            </span>
          </div>
        </div>
      </div>

      {/* 3. NARRATIVE */}
      <p className="text-[13px] text-muted-foreground leading-relaxed">{h.narrative}</p>

      {/* 4. DIVIDER + 5. COMPOSIÇÃO */}
      {h.composicao && <CompositionSection composicao={h.composicao} />}

      {/* 6. FOOTER */}
      <p className="text-[10px] text-muted-foreground/70 mt-4 pt-3 border-t border-border/40">
        não é recomendação de investimento. dados do informe mensal da CVM e do histórico de dividendos.
      </p>
    </div>
  )
}

// ============================================================================
// Composition subsection
// ============================================================================
function CompositionSection({ composicao }: { composicao: HealthData['composicao'] }) {
  if (!composicao) return null
  const b = composicao.breakdown

  // Collect visible segments (value > 0, not null)
  const segments = compCategories
    .map(c => ({
      ...c,
      value: b[c.key as keyof typeof b] ?? 0,
    }))
    .filter(s => s.value > 0)

  if (segments.length === 0) {
    return (
      <div className="mt-5 pt-4 border-t border-border/60">
        <div className="flex items-center gap-2">
          <h4 className="text-[13px] font-medium text-foreground">composição dos ativos</h4>
          <InfoTooltip label="Mais informações sobre composição dos ativos" title="composição dos ativos">
            distribuição do patrimônio do fundo entre as principais classes de ativos, conforme o informe mensal da CVM. não entra no cálculo do índice, mas ajuda a verificar se o fundo se comporta de acordo com a classificação declarada.
          </InfoTooltip>
        </div>
        <p className="text-[12px] text-muted-foreground mt-2">informação não disponível</p>
      </div>
    )
  }

  // Normalize to 100% for the bar (in case the fractions don't sum exactly to 1)
  const total = segments.reduce((s, x) => s + x.value, 0)

  return (
    <div className="mt-5 pt-4 border-t border-border/60">
      <div className="flex items-start justify-between gap-3 flex-wrap mb-3">
        <div className="flex items-center gap-2">
          <h4 className="text-[13px] font-medium text-foreground">composição dos ativos</h4>
          <InfoTooltip label="Mais informações sobre composição dos ativos" title="composição dos ativos">
            distribuição do patrimônio do fundo entre as principais classes de ativos, conforme o informe mensal da CVM. não entra no cálculo do índice, mas ajuda a verificar se o fundo se comporta de acordo com a classificação declarada.
          </InfoTooltip>
        </div>
        {composicao.classificacao_declarada && (
          <div className="text-[11px] font-mono text-muted-foreground">
            classificação declarada: <span className="text-foreground">{composicao.classificacao_declarada.toLowerCase()}</span>
          </div>
        )}
      </div>

      {/* Stacked bar */}
      <div className="flex w-full h-6 rounded-[6px] overflow-hidden bg-secondary/50" role="img" aria-label="Composição dos ativos">
        {segments.map(s => (
          <div
            key={s.key}
            style={{ width: `${(s.value / total) * 100}%`, background: s.color }}
            title={`${s.label}: ${Math.round(s.value * 100)}%`}
          />
        ))}
      </div>

      {/* Legend */}
      <ul className="flex flex-wrap gap-x-4 gap-y-1.5 mt-3">
        {segments.map(s => (
          <li key={s.key} className="flex items-center gap-2 text-[11px]">
            <span className="w-2.5 h-2.5 rounded-[2px]" style={{ background: s.color }} aria-hidden="true" />
            <span className="text-muted-foreground">{s.label}</span>
            <span className="text-foreground font-mono tabular-nums">{Math.round(s.value * 100)}%</span>
          </li>
        ))}
      </ul>
    </div>
  )
}
