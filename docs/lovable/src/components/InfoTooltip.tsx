import { useState, useRef, useEffect, ReactNode } from 'react'
import { createPortal } from 'react-dom'

interface InfoTooltipProps {
  label: string                // aria-label for the button
  title?: string                // optional bold header inside tooltip
  children: ReactNode           // tooltip body
  className?: string            // extra classes on the button (e.g., sizing/positioning overrides)
}

/**
 * Small (i) icon that opens a portal-positioned tooltip on click.
 * Works on desktop (click-to-toggle) and mobile (tap-to-toggle).
 * Closes on outside click, scroll, or resize.
 */
export function InfoTooltip({ label, title, children, className }: InfoTooltipProps) {
  const [open, setOpen] = useState(false)
  const [pos, setPos] = useState({ top: 0, left: 0 })
  const btnRef = useRef<HTMLButtonElement>(null)

  function handleOpen() {
    if (btnRef.current) {
      const rect = btnRef.current.getBoundingClientRect()
      const tooltipWidth = 280
      let left = rect.left
      if (left + tooltipWidth > window.innerWidth - 10) left = window.innerWidth - tooltipWidth - 10
      if (left < 10) left = 10
      // Position above the icon when near the bottom, else below
      const spaceBelow = window.innerHeight - rect.bottom
      const top = spaceBelow < 140 ? rect.top - 8 : rect.bottom + 8
      setPos({ top, left })
    }
    setOpen(o => !o)
  }

  useEffect(() => {
    if (!open) return
    const close = () => setOpen(false)
    window.addEventListener('scroll', close, true)
    window.addEventListener('resize', close)
    return () => {
      window.removeEventListener('scroll', close, true)
      window.removeEventListener('resize', close)
    }
  }, [open])

  const tooltip = open ? createPortal(
    <>
      <div className="fixed inset-0 z-[999]" onClick={() => setOpen(false)} />
      <div
        className="fixed z-[1000] w-[280px] max-w-[calc(100vw-20px)] bg-card border border-border rounded-[10px] p-3.5 shadow-2xl text-left"
        style={{ top: pos.top, left: pos.left }}
        role="tooltip"
      >
        {title && <p className="text-[12px] font-medium text-foreground mb-1.5">{title}</p>}
        <div className="text-[11px] leading-relaxed text-muted-foreground font-sans">
          {children}
        </div>
      </div>
    </>,
    document.body
  ) : null

  return (
    <>
      <button
        ref={btnRef}
        type="button"
        onClick={handleOpen}
        className={
          'w-[15px] h-[15px] rounded-full bg-secondary border border-border text-[9px] font-mono text-muted-foreground/80 inline-flex items-center justify-center hover:text-accent hover:border-accent/40 transition-colors flex-shrink-0 ' +
          (className ?? '')
        }
        aria-label={label}
      >
        i
      </button>
      {tooltip}
    </>
  )
}
