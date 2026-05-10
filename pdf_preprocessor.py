"""
pdf_preprocessor.py — Convert relatório gerencial PDFs into LLM-ready Markdown.

Why preprocess at all?
----------------------
We could send the raw PDF to Claude via the API's `document` block. That works
but is expensive: Claude bills the full PDF (text + image rendering of every
page) at base rates. Pre-extracting text-with-tables typically reduces token
count 40-50% with no loss of content for FII/FIAGRO relatórios, which are
universally machine-generated PDFs (text layer is reliable).

What this does
--------------
For each page:
  - Extract text in reading order
  - Detect tables and render them as Markdown tables
  - Insert a `## Page N` header so the agent and judge can cite sources
  - Drop pages with no extractable text (pure image/cover pages)

What this does NOT do
---------------------
  - OCR scanned PDFs. If the text layer is empty across all pages, we raise
    PDFHasNoTextError. The caller logs to `erros` and the fund is flagged
    for manual review. Adding OCR is a separate project.
  - Fix garbled text. If a PDF was generated badly (broken fonts, ligature
    issues), the output reflects that. Caller should sanity-check token
    counts against expected ranges.
  - Resolve images, charts, or non-tabular figures. Charts in FII relatórios
    are usually accompanied by tables with the same data; we extract the
    tables and skip the images.

Public API
----------
    preprocess_pdf(pdf_path: Path) -> str
        Returns Markdown string ready for the LLM.

    preprocess_pdf_with_stats(pdf_path: Path) -> tuple[str, dict]
        Returns (markdown, stats) where stats has page count, table count,
        char count, and any pages we had to skip.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pdfplumber


class PDFHasNoTextError(Exception):
    """Raised when a PDF has no extractable text on any page (likely scanned)."""


def _format_table_as_markdown(table: list[list[str | None]]) -> str:
    """
    Convert a pdfplumber-extracted table (list of rows, each a list of cells)
    into a Markdown table. pdfplumber sometimes returns None for empty cells
    and trailing whitespace — we normalize both.

    Edge cases:
      - Single-row "table" → render as a one-row table with no header separator
      - Empty table → return empty string (caller skips)
      - Cells with embedded newlines → replaced with spaces (Markdown tables
        can't render multi-line cells without HTML)
    """
    if not table or not any(any(c for c in row if c) for row in table):
        return ""

    def clean_cell(c: Any) -> str:
        if c is None:
            return ""
        s = str(c).strip()
        # Markdown tables break on embedded newlines and unescaped pipes
        return s.replace("\n", " ").replace("|", "\\|")

    rows = [[clean_cell(c) for c in row] for row in table]

    # Determine column count from the widest row (some rows may be shorter)
    n_cols = max(len(r) for r in rows)
    rows = [r + [""] * (n_cols - len(r)) for r in rows]

    # First row is the header (pdfplumber's convention; usually correct for FII reports)
    header = rows[0]
    body = rows[1:]

    lines = []
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| " + " | ".join(["---"] * n_cols) + " |")
    for r in body:
        lines.append("| " + " | ".join(r) + " |")
    return "\n".join(lines)


def _extract_page_text(page: pdfplumber.page.Page) -> str:
    """
    Extract text from a single page in roughly reading order. pdfplumber's
    extract_text() does this well by default; we just normalize whitespace.
    """
    raw = page.extract_text() or ""
    # Collapse runs of whitespace within lines, but preserve line breaks
    cleaned_lines = []
    for line in raw.split("\n"):
        s = " ".join(line.split())  # collapse internal whitespace
        if s:
            cleaned_lines.append(s)
    return "\n".join(cleaned_lines)


def _table_duplicates_text(table: list[list[str | None]], page_text: str,
                           threshold: float = 0.7) -> bool:
    """
    Return True if the table's content overlaps heavily with the page's
    flowing text. pdfplumber's default detector treats column-based layouts
    as tables, producing duplication with text we already extracted. Detect
    that case by checking how much of the table's text appears verbatim in
    the page text.
    """
    if not page_text:
        return False
    cells = []
    for row in table:
        for c in row:
            if c is None:
                continue
            s = str(c).strip()
            if len(s) >= 8:  # ignore short cells that are likely numbers/headers
                cells.append(s)
    if not cells:
        return False

    # Normalize whitespace on both sides for a fair comparison
    norm_page = " ".join(page_text.split())
    matches = sum(1 for c in cells if " ".join(c.split()) in norm_page)
    return matches / len(cells) >= threshold


def _extract_page_tables(page: pdfplumber.page.Page, page_text: str) -> list[str]:
    """
    Return Markdown-formatted tables from a page. Uses pdfplumber's default
    detection (which catches layout-based tables that gestores actually use)
    and then filters out tables whose content is already present in the
    flowing page text — those are layout artifacts, not real tables.
    """
    tables_raw = page.extract_tables() or []
    out = []
    for t in tables_raw:
        if _table_duplicates_text(t, page_text):
            continue
        md = _format_table_as_markdown(t)
        if md:
            out.append(md)
    return out


def preprocess_pdf_with_stats(pdf_path: Path) -> tuple[str, dict]:
    """
    Main entry point. Returns (markdown_string, stats_dict).

    stats_dict shape:
        {
            "pdf_path": str,
            "n_pages": int,
            "n_pages_with_text": int,
            "n_pages_skipped": int,        # pages with no extractable text
            "n_tables": int,
            "char_count": int,
            "estimated_tokens": int,        # rough: char_count / 4
        }

    Raises PDFHasNoTextError if no page has extractable text.
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    parts: list[str] = []
    n_pages = 0
    n_pages_with_text = 0
    n_pages_skipped = 0
    n_tables = 0

    with pdfplumber.open(pdf_path) as pdf:
        n_pages = len(pdf.pages)
        for i, page in enumerate(pdf.pages, start=1):
            text = _extract_page_text(page)
            tables = _extract_page_tables(page, text)

            if not text and not tables:
                n_pages_skipped += 1
                continue

            n_pages_with_text += 1
            n_tables += len(tables)

            section: list[str] = [f"## Page {i}", ""]

            if text:
                section.append(text)

            if tables:
                section.append("")  # blank line before tables
                for j, t in enumerate(tables, start=1):
                    section.append(f"_Table {i}.{j}_")
                    section.append("")
                    section.append(t)
                    section.append("")

            parts.append("\n".join(section))

    if n_pages_with_text == 0:
        raise PDFHasNoTextError(
            f"No extractable text in {pdf_path.name} ({n_pages} pages). "
            "PDF may be scanned/image-only — manual review required."
        )

    markdown = "\n\n".join(parts).strip() + "\n"

    stats = {
        "pdf_path": str(pdf_path),
        "n_pages": n_pages,
        "n_pages_with_text": n_pages_with_text,
        "n_pages_skipped": n_pages_skipped,
        "n_tables": n_tables,
        "char_count": len(markdown),
        # Rough token estimate: ~4 chars per token for Portuguese text.
        # Real count comes from the API response; this is for budgeting.
        "estimated_tokens": len(markdown) // 4,
    }
    return markdown, stats


def preprocess_pdf(pdf_path: Path) -> str:
    """Thin convenience wrapper when you don't need the stats."""
    md, _ = preprocess_pdf_with_stats(pdf_path)
    return md


if __name__ == "__main__":
    # CLI for quick local testing:
    #   python pdf_preprocessor.py /path/to/some.pdf
    #   python pdf_preprocessor.py /path/to/some.pdf --out /path/to/out.md
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess a relatório PDF to Markdown.")
    parser.add_argument("pdf_path", type=Path)
    parser.add_argument("--out", type=Path, help="Write Markdown to this file instead of stdout")
    parser.add_argument("--stats-only", action="store_true",
                        help="Print stats only, don't emit the Markdown body")
    args = parser.parse_args()

    md, stats = preprocess_pdf_with_stats(args.pdf_path)

    if args.stats_only:
        for k, v in stats.items():
            print(f"{k}: {v}")
    elif args.out:
        args.out.write_text(md, encoding="utf-8")
        print(f"Wrote {len(md):,} chars to {args.out}")
        print(f"Stats: {stats}")
    else:
        print(md)
