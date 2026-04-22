import { useState, KeyboardEvent } from 'react'
import { Search } from 'lucide-react'

interface SearchBarProps {
  onSearch: (ticker: string) => void
  initialValue?: string
}

export function SearchBar({ onSearch, initialValue = '' }: SearchBarProps) {
  const [value, setValue] = useState(initialValue)
  const [error, setError] = useState('')
  const [focused, setFocused] = useState(false)

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      const ticker = value.trim().toUpperCase()
      if (!ticker) { setError('Digite um ticker válido'); return }
      setError('')
      onSearch(ticker)
    }
  }

  return (
    <div className="border-b border-border bg-card/50 backdrop-blur-sm px-4 sm:px-7 py-4 transition-colors">
      <div className="max-w-[920px] mx-auto">
        <div className={`relative flex items-center gap-3 rounded-xl border bg-card px-4 h-[46px] transition-all duration-200 ${focused ? 'border-primary/40 shadow-[0_0_16px_-4px_hsl(var(--primary)/0.2)]' : 'border-border'}`}>
          <Search className="w-[18px] h-[18px] text-muted-foreground flex-shrink-0" strokeWidth={1.75} />
          <input
            type="text"
            className="flex-1 bg-transparent border-none outline-none text-sm text-foreground font-mono placeholder:text-muted-foreground/50 h-full"
            placeholder="Buscar fundo — ex: MXRF11"
            value={value}
            onChange={(e) => setValue(e.target.value)}
            onKeyDown={handleKeyDown}
            onFocus={() => setFocused(true)}
            onBlur={() => setFocused(false)}
          />
          <kbd className="hidden sm:inline-flex text-[10px] font-mono border border-border px-2 py-0.5 rounded-md text-muted-foreground bg-secondary">
            Enter ↵
          </kbd>
        </div>
        {error && <p className="text-[11px] text-loss font-mono mt-2">{error}</p>}
      </div>
    </div>
  )
}
