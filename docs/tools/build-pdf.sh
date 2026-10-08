#!/usr/bin/env bash
# Build a PDF from one of the repo-root planning documents.
#   pixi run pdf ../../V2_FEATURE_REQUESTS.md [../../other.md ...]
# Output lands next to the source as <name>.pdf. Mermaid blocks are replaced by
# figures/<name>.png (see mermaid-figure.lua); render those first.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
fontdir="${CONDA_PREFIX:?run via pixi so CONDA_PREFIX is set}/fonts"
if [ "$#" -eq 0 ]; then
    echo "usage: $0 <markdown file> [...]" >&2
    exit 2
fi
for src in "$@"; do
    src="$(realpath "$src")"
    dir="$(dirname "$src")"
    stem="$(basename "${src%.md}")"
    title="$(sed -n '1s/^# //p' "$src")"
    (
        cd "$dir"
        pandoc "$src" \
            --from gfm+attributes+raw_html \
            --to pdf \
            --pdf-engine=tectonic \
            --lua-filter="$here/mermaid-figure.lua" \
            --variable header-includes="\\newcommand{\\fontdir}{$fontdir}" \
            --include-in-header="$here/pdf-header.tex" \
            --metadata title="$title" \
            --metadata figure_stem="$stem" \
            --metadata date="$(date +%Y-%m-%d)" \
            --variable geometry:margin=2.2cm \
            --variable papersize=a4 \
            --variable colorlinks=true \
            --variable linkcolor=blue \
            --variable urlcolor=blue \
            --variable toccolor=black \
            --shift-heading-level-by=-1 \
            --output "$stem.pdf"
    )
    echo "$dir/$stem.pdf"
done
