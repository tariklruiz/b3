import { type FundData, type ProfileComposicao } from '@/lib/fii-helpers'

// ============================================================================
// Categories — order in the bar (left-to-right), label, visibility rules
//
// Each category has a fixed color from a 7-tone categorical palette.
// Tones picked to be distinguishable in both light and dark themes,
// avoiding the brand accent (which is reserved for actions/highlights).
// ============================================================================
interface Category {
  key: keyof ProfileComposicao
  label: string
  color: string
  alwaysShowIfPositive?: boolean   // FII / ações soc must render even tiny
}

const CATEGORIES: Category[] = [
  { key: 'cri_cra_pct',                    label: 'CRI/CRA',              color: '#3b82f6' /* blue */            },
  { key: 'titulos_privados_pct',           label: 'Títulos privados',     color: '#06b6d4' /* cyan */            },
  { key: 'fundos_renda_fixa_pct',          label: 'Fundos RF',            color: '#f59e0b' /* amber */           },
  { key: 'imoveis_renda_pct',              label: 'Imóveis renda',        color: '#10b981' /* emerald */         },
  { key: 'fii_pct',                        label: 'FII',                  color: '#a855f7' /* purple */,         alwaysShowIfPositive: true },
  { key: 'acoes_sociedades_ativ_fii_pct',  label: 'Ações soc. ativ. FII', color: '#ec4899' /* pink */,           alwaysShowIfPositive: true },
  { key: 'outros_pct',                     label: 'Outros',               color: '#94a3b8' /* slate */           },
]

// ============================================================================
// Helpers
// ============================================================================
function fmtCompetencia(isoDate: string | null): string {
  if (!isoDate) return ''
  const [y, m] = isoDate.split('-')
  return `${m}/${y}`
}

function fmtPct(v: number): string {
  // Round to nearest int %
  return Math.round(v * 100) + '%'
}

interface VisibleSegment {
  key: string
  label: string
  color: string
  value: number
  alwaysShow: boolean
}

function getVisibleSegments(c: ProfileComposicao): VisibleSegment[] {
  return CATEGORIES
    .map(cat => ({
      key: cat.key,
      label: cat.label,
      color: cat.color,
      value: c[cat.key] ?? 0,
      alwaysShow: !!cat.alwaysShowIfPositive,
    }))
    .filter(s => {
      if (s.value <= 0) return false
      // 'outros' has a 0.5% floor — sub-noise hidden
      if (s.key === 'outros_pct' && s.value <= 0.005) return false
      return true
    })
}

// ============================================================================
// Card shell — matches the rest of the page
// ============================================================================
function CardShell({ children }: { children: React.ReactNode }) {
  return (
    <div className="relative bg-card border border-border rounded-xl p-6 sm:p-8 shadow-sm transition-colors overflow-hidden">
      <div className="absolute top-0 left-0 right-0 h-[2px] bg-gradient-to-r from-primary via-accent to-primary" />
      {children}
    </div>
  )
}

function Header({ classificacaoLowercase, competencia }: {
  classificacaoLowercase?: string
  competencia?: string
}) {
  return (
    <>
      <div className="flex items-center gap-1.5 mb-1">
        <span className="text-[10px] text-primary uppercase tracking-[0.15em] font-semibold whitespace-nowrap">Perfil do fundo</span>
        <div className="flex-1 h-px bg-gradient-to-r from-border to-transparent min-w-[24px]" />
      </div>
      {(classificacaoLowercase || competencia) && (
        <p className="text-[12px] text-muted-foreground mb-4">
          {competencia && <>composição em {competencia}</>}
          {competencia && classificacaoLowercase && <> · </>}
          {classificacaoLowercase && <>classificação declarada: <span className="text-foreground">{classificacaoLowercase}</span></>}
        </p>
      )}
    </>
  )
}

// ============================================================================
// Main component
// ============================================================================
export function ProfileCard({ fund }: { fund: FundData }) {
  const p = fund.profile

  // -- Empty state --
  if (!p) {
    return (
      <CardShell>
        <Header />
        <p className="text-[13px] text-foreground mb-2">composição não disponível para este fundo</p>
        <p className="text-[12px] text-muted-foreground">
          isso geralmente acontece com fundos novos ou que ainda não publicaram informe mensal na CVM.
        </p>
      </CardShell>
    )
  }

  const compet = fmtCompetencia(p.competencia)
  const segments = getVisibleSegments(p.composicao)
  const total = segments.reduce((s, x) => s + x.value, 0) || 1

  // Sort descending by value — bar AND legend use the same order
  const sortedByValue = [...segments].sort((a, b) => b.value - a.value)

  const segmentsForBar = sortedByValue.map(s => ({
    ...s,
    pct: (s.value / total) * 100,
  }))

  const legendSorted = sortedByValue

  return (
    <CardShell>
      <Header
        classificacaoLowercase={[
          p.classificacao_declarada,
          p.subclassificacao_declarada,
        ].filter(Boolean).map(s => s!.toLowerCase()).join(' · ')}
        competencia={compet}
      />

      {/* Stacked bar */}
      <div
        className="flex w-full h-6 rounded-md overflow-hidden bg-secondary/40 border border-border/50"
        role="img"
        aria-label={
          'Composição: ' +
          legendSorted.map(s => `${s.label} ${fmtPct(s.value)}`).join(', ')
        }
      >
        {segmentsForBar.map(s => (
          <div
            key={s.key}
            className="h-full"
            style={{
              flex: `${s.pct} ${s.pct} 0`,
              minWidth: s.alwaysShow ? '4px' : 0,
              backgroundColor: s.color,
            }}
            title={`${s.label}: ${fmtPct(s.value)}`}
          />
        ))}
      </div>

      {/* Legend */}
      <ul className="grid grid-cols-2 sm:flex sm:flex-wrap gap-x-5 gap-y-2 mt-4">
        {legendSorted.map(s => (
          <li key={s.key} className="inline-flex items-center gap-2 text-[12px]">
            <span
              className="w-2.5 h-2.5 rounded-[2px] flex-shrink-0"
              style={{ backgroundColor: s.color }}
              aria-hidden="true"
            />
            <span className="text-muted-foreground">{s.label}</span>
            <span className="text-foreground font-mono tabular-nums tracking-tight">{fmtPct(s.value)}</span>
          </li>
        ))}
      </ul>

      {/* Footer */}
      <p className="text-[10px] text-muted-foreground/70 mt-5 pt-3 border-t border-border/40">
        fonte: informe mensal CVM{compet && ` · ${compet}`}
      </p>
    </CardShell>
  )
}
