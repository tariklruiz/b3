import { useState, KeyboardEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { Sun, Moon } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'
import logoLight from '@/assets/logo-light.svg'
import logoDark from '@/assets/logo-dark.svg'
import '@/styles/homepage.css'

// Popular tickers (static for v1 — can be wired to GET /funds/popular later)
const POPULAR = ['MXRF11', 'KNCR11', 'HGLG11', 'XPML11']

export default function Home() {
  const { isDark, toggle } = useTheme()
  const navigate = useNavigate()
  const [query, setQuery] = useState('')

  const goToFund = (ticker: string) => {
    const t = ticker.trim().toUpperCase()
    if (!t) return
    navigate(`/fundo/${t}`)
  }

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') goToFund(query)
  }

  return (
    <div className="fg-home min-h-screen bg-background transition-colors duration-300">
      {/* ========== HEADER ========== */}
      <header className="site-header">
        <div className="site-header-inner">
          <a href="/" className="brand" aria-label="FII Guia">
            <img src={isDark ? logoDark : logoLight} alt="FII Guia" />
          </a>
          <nav className="nav" aria-label="Navegação principal">
            <a href="#what">O que é um FII</a>
            <a href="#context">Dados com contexto</a>
            <a href="#ai">Resumo do gestor</a>
            <a href="#principles">Princípios</a>
          </nav>
          <button
            className="theme-toggle"
            onClick={toggle}
            aria-label={isDark ? 'Mudar para tema claro' : 'Mudar para tema escuro'}
          >
            {isDark ? <Sun className="w-[13px] h-[13px]" /> : <Moon className="w-[13px] h-[13px]" />}
            <span>{isDark ? 'tema claro' : 'tema escuro'}</span>
          </button>
        </div>
      </header>

      {/* ========== STICKY SEARCH BAND ========== */}
      <div className="search-band">
        <div className="search-band-inner">
          <div className="hero-search">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" aria-hidden="true">
              <circle cx="11" cy="11" r="7" />
              <path d="m21 21-4.35-4.35" />
            </svg>
            <input
              type="text"
              placeholder="Buscar fundo — ex: MXRF11"
              autoComplete="off"
              value={query}
              onChange={e => setQuery(e.target.value)}
              onKeyDown={onKeyDown}
              aria-label="Buscar fundo imobiliário por ticker"
            />
          </div>
          <div className="search-hint band">
            <span className="lbl">populares:</span>
            {POPULAR.slice(0, 3).map(t => (
              <button key={t} className="chip" onClick={() => goToFund(t)}>{t}</button>
            ))}
          </div>
        </div>
      </div>

      {/* ========== HERO ========== */}
      <section className="hero">
        <div className="container">
          <div className="hero-grid">
            <div className="hero-copy">
              <span className="eyebrow">
                <span className="dot" />
                gratuito · sem cadastro · sem afiliação · dados oficiais d-1
              </span>
              <h1 className="hero-title">
                Entenda <span className="em">antes</span> de investir<br />em fundos imobiliários.
              </h1>
              <p className="hero-sub">
                Pesquise qualquer FII da B3 e veja uma análise completa — <b>cada métrica traduzida, cada número com contexto</b>. Sem recomendações, sem ranking genérico, sem link de corretora.
              </p>

              <div className="hero-search-block">
                <div className="hero-search">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" aria-hidden="true">
                    <circle cx="11" cy="11" r="7" />
                    <path d="m21 21-4.35-4.35" />
                  </svg>
                  <input
                    type="text"
                    placeholder="Buscar fundo — ex: MXRF11"
                    autoComplete="off"
                    value={query}
                    onChange={e => setQuery(e.target.value)}
                    onKeyDown={onKeyDown}
                    aria-label="Buscar fundo imobiliário por ticker"
                  />
                  <span className="kbd">Enter ↵</span>
                </div>
                <div className="search-hint">
                  <span className="lbl">populares:</span>
                  {POPULAR.map(t => (
                    <button key={t} className="chip" onClick={() => goToFund(t)}>{t}</button>
                  ))}
                </div>
              </div>

              <div className="hero-pitch-list">
                <div className="item">
                  <span className="mk">✓</span>
                  <div>Toda métrica vem com <b>uma frase em português claro</b> — P/VP 1,03 vira “você paga R$ 1,03 por R$ 1 de patrimônio.”</div>
                </div>
                <div className="item">
                  <span className="mk">✓</span>
                  <div>Comparações <b>dentro do segmento</b> — um fundo de papel não é medido contra um de tijolo.</div>
                </div>
                <div className="item">
                  <span className="mk">✓</span>
                  <div>Resumo do gestor <b>extraído por IA</b> do relatório gerencial mensal — o que mudou no caixa, no portfólio e nos riscos.</div>
                </div>
              </div>
            </div>

            {/* ---- Hero preview mini-card ---- */}
            <div className="hero-preview" aria-hidden="true">
              <div className="hero-preview-chrome">
                <span>fiiguia.com.br/fundo/mxrf11</span>
                <span className="dots"><i /><i /><i /></span>
              </div>
              <div className="preview-card">
                <div className="pc-head">
                  <div>
                    <div className="pc-ticker">MXRF11</div>
                    <div className="pc-name">fii maxi renda · papel · CDI+</div>
                  </div>
                  <span className="tag low"><span className="dot" />risco baixo</span>
                </div>

                <div className="pc-price-row">
                  <span className="pc-price tabular">R$ 9,87</span>
                  <span className="pc-date">fechamento · 16/04/2026</span>
                </div>

                <div className="pc-seclabel">
                  <span className="t">P/VP</span>
                  <span className="line" />
                </div>

                <div className="pvp-headline">
                  <span className="num tabular">1,03</span>
                  <span className="desc">prêmio de <b>3%</b> sobre o patrimônio líquido</span>
                </div>
                <div className="pvp-bar">
                  <div className="fill" />
                  <div className="tick" />
                </div>
                <div className="pvp-scale">
                  <span>0,70 desconto</span>
                  <span>1,00 justo</span>
                  <span>1,30 prêmio</span>
                </div>

                <div className="pc-callout">
                  <div className="ic">i</div>
                  <div className="txt">
                    <b>Em português:</b> você pagaria <em>R$ 1,03</em> por cada <em>R$ 1</em> de patrimônio. É um prêmio pequeno — dentro da média histórica do setor de papel.
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ========== O QUE É UM FII ========== */}
      <section className="section" id="what">
        <div className="container">
          <div className="section-head">
            <span className="kicker accent">começar do zero</span>
            <h2>O que é um fundo imobiliário, em <span className="em">três ideias</span>.</h2>
            <p>Antes de abrir qualquer cotação: o que você está comprando, como o dinheiro chega na sua conta, e o que pode dar errado.</p>
          </div>

          <div className="explain-grid">
            <div className="explain">
              <div className="num">01 — O que você compra</div>
              <div className="ill">
                <div className="ill-quotas">
                  <div className="qbuild">
                    {Array.from({ length: 20 }).map((_, i) => (
                      <div key={i} className={`q${i === 18 ? ' mine' : ''}`} />
                    ))}
                  </div>
                  <div className="qlabel">sua cota</div>
                </div>
              </div>
              <h3>Um pedacinho de muitos imóveis.</h3>
              <p>Um FII junta o dinheiro de milhares de investidores e compra <b>shoppings, galpões logísticos, lajes corporativas</b> — ou títulos atrelados a imóveis. Sua cota representa sua parte nesse bolo.</p>
            </div>

            <div className="explain">
              <div className="num">02 — Como você ganha</div>
              <div className="ill">
                <div className="ill-pocket">
                  <div className="pocket"><div className="coin">$</div></div>
                  <div className="plabel"><b>+R$</b><br />todo mês</div>
                </div>
              </div>
              <h3>Aluguel, todo mês, direto na conta.</h3>
              <p>Por lei, os FIIs distribuem <b>no mínimo 95%</b> dos lucros aos cotistas. Na prática, você recebe um rendimento mensal — tipo aluguel — <b>isento de imposto de renda</b> para pessoa física.</p>
            </div>

            <div className="explain">
              <div className="num">03 — O risco real</div>
              <div className="ill">
                <div className="ill-amp">
                  <div className="track">
                    <div className="t-lbl"><b>cota</b><span>varia</span></div>
                    <div className="t-range">
                      <div className="t-rail" />
                      <div className="t-band" style={{ left: '8%', right: '8%' }} />
                      <div className="t-dot" style={{ left: '10%' }} />
                      <div className="t-dot" style={{ left: '90%' }} />
                      <div className="t-dot live" style={{ left: '58%' }} />
                    </div>
                    <div className="t-amp">±18%</div>
                  </div>
                  <div className="track div">
                    <div className="t-lbl"><b>dividendo</b><span>firme</span></div>
                    <div className="t-range">
                      <div className="t-rail" />
                      <div className="t-band" style={{ left: '44%', right: '44%' }} />
                      <div className="t-dot" style={{ left: '46%' }} />
                      <div className="t-dot" style={{ left: '54%' }} />
                      <div className="t-dot live" style={{ left: '50%' }} />
                    </div>
                    <div className="t-amp">±2%</div>
                  </div>
                  <div className="foot">amplitude · últimos 12 meses</div>
                </div>
              </div>
              <h3>A cota oscila. O rendimento também.</h3>
              <p>Diferente da poupança, o <b>valor da cota sobe e desce</b> diariamente, e o rendimento pode variar (inquilinos saem, contratos mudam, crédito piora). Entender isso é o primeiro passo — e é pra isso que o Guia existe.</p>
            </div>
          </div>
        </div>
      </section>

      {/* ========== DADOS COM CONTEXTO ========== */}
      <section className="section" id="context">
        <div className="container">
          <div className="section-head">
            <span className="kicker accent">a diferença do fii guia</span>
            <h2>Dados com <span className="em">contexto</span>. Não dados jogados na sua cara.</h2>
            <p>Um número sozinho não decide nada. Toda métrica aqui vem com uma frase explicando o que significa, como comparar, e o que observar.</p>
          </div>
        </div>
      </section>

      {/* ========== AI GESTOR ========== */}
      <section className="section" id="ai">
        <div className="container">
          <div className="section-head">
            <span className="kicker accent">recurso em destaque</span>
            <h2>Todo mês, a IA lê o relatório do gestor — e te conta o que <span className="em">mudou</span>.</h2>
            <p>Relatórios gerenciais de FIIs são longos, em PDF, e escritos em linguagem de gestora. O Guia usa Claude Sonnet pra extrair <b>o que importa</b>: resultado por cota, distribuição, reserva de caixa, CRIs em observação, mudanças no portfólio.</p>
          </div>
        </div>
      </section>

      {/* ========== PRINCÍPIOS ========== */}
      <section className="section" id="principles">
        <div className="container">
          <div className="section-head">
            <span className="kicker accent">como nos mantemos isentos</span>
            <h2>Quatro <span className="em">princípios</span> que não negociamos.</h2>
            <p>Você merece saber como as análises são construídas — e, principalmente, o que <b>nunca</b> entra nelas.</p>
          </div>

          <div className="principles-grid">
            <div className="principle">
              <div className="mark">01</div>
              <div>
                <h4>Sem recomendação de compra</h4>
                <p>Não existe “esse FII é bom”. Existe “esse FII se encaixa se você procura X, Y, Z”. A decisão é sempre sua.</p>
              </div>
            </div>
            <div className="principle">
              <div className="mark">02</div>
              <div>
                <h4>Fontes públicas e auditáveis</h4>
                <p>Todos os dados vêm da B3, CVM e relatórios gerenciais dos próprios fundos. Cada número tem a data de referência visível.</p>
              </div>
            </div>
            <div className="principle">
              <div className="mark">03</div>
              <div>
                <h4>Sem afiliação com gestoras</h4>
                <p>Não recebemos pagamento de gestoras, corretoras ou plataformas. Não há FII em destaque comercial ou lista paga.</p>
              </div>
            </div>
            <div className="principle">
              <div className="mark">04</div>
              <div>
                <h4>Dados com contexto</h4>
                <p>Toda métrica vem com uma frase em português claro explicando o que significa e como comparar. Número sozinho não decide nada.</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ========== FOOTER ========== */}
      <footer className="site-footer">
        <div className="container">
          <div className="footer-grid">
            <div className="footer-col">
              <a href="/" className="brand" aria-label="FII Guia">
                <img src={isDark ? logoDark : logoLight} alt="FII Guia" />
              </a>
              <p className="footer-about">
                Um guia gratuito e independente pra quem quer entender fundos imobiliários antes de investir. Dados com contexto, linguagem clara, zero viés.
              </p>
            </div>
            <div className="footer-col">
              <h5>Sobre</h5>
              <a href="#principles">Princípios</a>
              <a href="#">Quem fez</a>
              <a href="#">Contato</a>
            </div>
          </div>
          <div className="footer-bottom">
            <div>© {new Date().getFullYear()} FII Guia · feito no Brasil</div>
            <div style={{ display: 'flex', gap: 20 }}>
              <a href="#">Privacidade</a>
              <a href="#">Termos</a>
            </div>
          </div>
        </div>
        <div className="disclaimer">
          <b>AVISO:</b> o FII Guia tem caráter exclusivamente educacional e informativo. Nenhum conteúdo aqui constitui recomendação de investimento, oferta de valores mobiliários ou consultoria. Rentabilidade passada não é garantia de rentabilidade futura. Antes de investir, consulte um profissional certificado e leia os documentos oficiais de cada fundo na CVM.
        </div>
      </footer>
    </div>
  )
}
