export const fmtPct = (v: number | null) =>
  v == null ? '—' : (v >= 0 ? '+' : '') + (v * 100).toFixed(2) + '%'

export const fmtPctAbs = (v: number | null) =>
  v == null ? '—' : (v * 100).toFixed(1) + '%'

export const fmtBRL = (v: number | null) =>
  v == null ? '—' : 'R$ ' + v.toFixed(3).replace('.', ',')

export const fmtK = (v: number | null) =>
  v == null ? '—' : v >= 1000 ? (v / 1000).toFixed(1) + 'k' : Math.round(v).toString()

export function pctColor(v: number) {
  if (v > 0.001) return 'text-profit'
  if (v < -0.001) return 'text-loss'
  return 'text-muted-foreground'
}

export function volBadge(v: number): [string, string] {
  if (v < 0.15) return ['profit', 'baixa']
  if (v < 0.25) return ['amber', 'média']
  return ['loss', 'alta']
}

export function liqBadge(v: number): [string, string] {
  if (v > 20000) return ['profit', 'alta']
  if (v > 5000) return ['amber', 'média']
  return ['loss', 'baixa']
}

export function riscoBadge(r: string): [string, string] {
  if (r === 'baixo') return ['profit', 'risco baixo']
  if (r === 'medio') return ['amber', 'risco médio']
  return ['loss', 'risco alto']
}

export function waffleColor(cur: number | null, prev: number | null) {
  if (cur === null) return 'hsl(var(--loss))'
  if (prev === null || cur > prev) return 'hsl(var(--profit))'
  if (cur < prev) return 'hsl(var(--amber))'
  return 'hsl(var(--info-blue))'
}

export function fmtNome(s: string | null) {
  if (!s) return '—'
  return s.replace(/\w\S*/g, w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
}

export function formatDate(s: string | null) {
  if (!s) return '—'
  const parts = s.split('-')
  if (parts.length === 3) return `${parts[2]}/${parts[1]}/${parts[0]}`
  return s
}

export function fmtMes(s: string | null) {
  if (!s) return '—'
  const months = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']
  const parts = s.split('-')
  if (parts.length >= 2) return `${months[parseInt(parts[1]) - 1]}/${parts[0].slice(2)}`
  return s
}

export interface FundData {
  ticker: string
  nome: string
  segmento: string
  administrador: string | null
  cotistas: number | null
  competencia: string | null
  risco: string
  atualizado: string

  preco: number
  preco_data: string
  preco_d1: number
  preco_d7: number
  preco_d30: number
  preco_d90: number
  preco_d180: number
  preco_12m: number
  preco_24m: number

  pvp: number | null

  vol_7d: number
  vol_30d: number
  vol_90d: number
  vol_12m: number

  liq_7d: number
  liq_30d: number
  liq_90d: number
  liq_12m: number

  div_valor: number
  div_avg12: number | null
  div_pago_em: string
  div_base: string
  div_pagos: number
  div_total: number

  div_historico: { m: string; v: number | null }[]

  dy_anual: number
  _div_yoy: number
  bench_hint: string | null

  cdi_anual: number

  gestor_resumo: string | null
  gestor_tom: string | null
  gestor_mes: string | null
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function calcRisco(div: any, vol: any, preco: any): string {
  let score = 0
  const v90 = vol['90d'] ?? 0
  if (v90 > 0.25) score += 2
  else if (v90 > 0.15) score += 1

  const consistency = div.consistencia.pct ?? 1
  if (consistency < 0.85) score += 2
  else if (consistency < 1.00) score += 1

  const divYoy = div._yoy_valor ?? 0
  if (divYoy < -0.10) score += 2
  else if (divYoy < -0.03) score += 1

  const liq30 = preco.liquidez?.['30d'] ?? 0
  if (liq30 < 5000) score += 2
  else if (liq30 < 20000) score += 1

  const v = preco.variacao
  const d30 = v.d30 ?? 0
  if (d30 < -0.11) score += 2
  else if (d30 < -0.07) score += 1

  const d180 = v.d180 ?? 0
  if (d180 < -0.12) score += 2
  else if (d180 < -0.08) score += 1

  const d12m = v['12m'] ?? 0
  if (d12m < -0.13) score += 2
  else if (d12m < -0.09) score += 1

  const d24m = v['24m'] ?? 0
  if (d24m < -0.14) score += 2
  else if (d24m < -0.10) score += 1

  if (score <= 3) return 'baixo'
  else if (score <= 7) return 'medio'
  else return 'alto'
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function buildFundData(ticker: string, preco: any, div: any, informe: any, benchData: any, cdiData: any): FundData {
  const CDI_MENSAL = 0.0087
  const v = preco.variacao
  const vol = preco.volatilidade
  const liq = preco.liquidez
  const hoje = new Date().toLocaleDateString('pt-BR')

  const pvp = (informe && informe.nav_cota && preco.preco)
    ? preco.preco / informe.nav_cota
    : null

  const segmento = informe
    ? [informe.classificacao, informe.subclassificacao].filter(Boolean).join(' · ')
    : '—'

  const hist = div.historico
  if (hist.length >= 2) {
    const last = hist[hist.length - 1].valor_provento
    const first = hist[0].valor_provento
    div._yoy_valor = (first > 0) ? (last - first) / first : 0
  } else {
    div._yoy_valor = 0
  }

  const benchmarks = benchData ? benchData.benchmarks : {}
  const cdiMensal = cdiData ? cdiData.cdi_mensal : CDI_MENSAL
  const classificacao = informe ? informe.classificacao : null
  const bench = classificacao ? benchmarks[classificacao] : null
  const benchHint = bench
    ? `mediana ${classificacao}: ${fmtPctAbs(bench.mediana_dy_mensal)}/mês · ${fmtPctAbs(bench.mediana_dy_anual)}/ano (${bench.n_fundos} fundos)`
    : null

  return {
    ticker,
    nome: informe ? fmtNome(informe.nome) : ticker,
    segmento,
    administrador: informe ? informe.administrador : null,
    cotistas: informe ? informe.cotistas : null,
    competencia: informe ? informe.competencia : null,
    risco: calcRisco(div, preco.volatilidade, preco),
    atualizado: hoje,

    preco: preco.preco,
    preco_data: formatDate(preco.preco_data),
    preco_d1: v.d1 ?? 0,
    preco_d7: v.d7 ?? 0,
    preco_d30: v.d30 ?? 0,
    preco_d90: v.d90 ?? 0,
    preco_d180: v.d180 ?? 0,
    preco_12m: v['12m'] ?? 0,
    preco_24m: v['24m'] ?? 0,

    pvp,

    vol_7d: vol['7d'] ?? 0,
    vol_30d: vol['30d'] ?? 0,
    vol_90d: vol['90d'] ?? 0,
    vol_12m: vol['12m'] ?? 0,

    liq_7d: liq['7d'] ?? 0,
    liq_30d: liq['30d'] ?? 0,
    liq_90d: liq['90d'] ?? 0,
    liq_12m: liq['12m'] ?? 0,

    div_valor: div.ultimo.valor,
    div_avg12: div.historico.length > 0
      ? div.historico.reduce((s: number, d: { valor_provento: number }) => s + (d.valor_provento || 0), 0) / div.historico.length
      : div.ultimo.valor,
    div_pago_em: div.ultimo.data_pago || '—',
    div_base: div.ultimo.data_base || '—',
    div_pagos: div.consistencia.pagos,
    div_total: div.consistencia.total,

    div_historico: div.historico.map((d: { data_base: string; valor_provento: number | null }) => ({
      m: fmtMes(d.data_base),
      v: d.valor_provento,
    })),

    dy_anual: div.dy.anual ?? 0,
    _div_yoy: div._yoy_valor ?? 0,
    bench_hint: benchHint,

    cdi_anual: cdiData ? cdiData.cdi_anual : CDI_MENSAL * 12,

    gestor_resumo: null,
    gestor_tom: null,
    gestor_mes: null,
  }
}

export function buildRiskSignals(f: FundData) {
  function sigScore(val: number | null, medThresh: number, hiThresh: number) {
    if (val === null || val === undefined) return null
    if (val <= hiThresh) return 2
    if (val <= medThresh) return 1
    return 0
  }
  function sigScoreHigh(val: number | null, medThresh: number, hiThresh: number) {
    if (val === null || val === undefined) return null
    if (val >= hiThresh) return 2
    if (val >= medThresh) return 1
    return 0
  }
  function liqScoreInv(val: number | null) {
    if (val === null || val === undefined) return null
    if (val < 5000) return 2
    if (val < 20000) return 1
    return 0
  }

  const consistency = f.div_total > 0 ? f.div_pagos / f.div_total : 1

  return [
    { name: 'Volatilidade 90d', val: f.vol_90d, score: sigScoreHigh(f.vol_90d, 0.15, 0.25), fmt: (v: number) => fmtPctAbs(v), low: '< 15%', med: '15–25%', hi: '> 25%' },
    { name: 'Consistência div.', val: consistency, score: sigScoreHigh(1 - consistency, 0, 0.15), fmt: (v: number) => Math.round(v * 100) + '%', low: '100%', med: '85–99%', hi: '< 85%' },
    { name: 'Dividendo YoY', val: f._div_yoy, score: sigScore(f._div_yoy, -0.03, -0.10), fmt: (v: number) => fmtPct(v), low: '> -3%', med: '-3% a -10%', hi: '< -10%' },
    { name: 'Liquidez 30d', val: f.liq_30d, score: liqScoreInv(f.liq_30d), fmt: (v: number) => fmtK(Math.round(v)), low: '> 20k', med: '5k–20k', hi: '< 5k' },
    { name: 'Preço D-30', val: f.preco_d30, score: sigScore(f.preco_d30, -0.07, -0.11), fmt: (v: number) => fmtPct(v), low: '> -3%', med: '-3% a -7%', hi: '< -11%' },
    { name: 'Preço D-180', val: f.preco_d180, score: sigScore(f.preco_d180, -0.08, -0.12), fmt: (v: number) => fmtPct(v), low: '> -4%', med: '-4% a -8%', hi: '< -12%' },
    { name: 'Preço 12M', val: f.preco_12m, score: sigScore(f.preco_12m, -0.09, -0.13), fmt: (v: number) => fmtPct(v), low: '> -5%', med: '-5% a -9%', hi: '< -13%' },
    { name: 'Preço 24M', val: f.preco_24m, score: sigScore(f.preco_24m, -0.10, -0.14), fmt: (v: number) => fmtPct(v), low: '> -6%', med: '-6% a -10%', hi: '< -14%' },
  ]
}
