"""Rasterize a PDF to one PNG per page: python render.py deck.pdf outdir"""

import sys
from pathlib import Path

import pymupdf

pdf, out = Path(sys.argv[1]), Path(sys.argv[2])
out.mkdir(exist_ok=True)
for stale in out.glob("page-*.png"):
    stale.unlink()
with pymupdf.open(pdf) as document:
    for index, page in enumerate(document, start=1):
        page.get_pixmap(dpi=110).save(out / f"page-{index:02d}.png")
    print(f"{len(document)} pages -> {out}")
