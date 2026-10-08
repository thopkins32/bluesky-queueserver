"""Render the named ```mermaid blocks of a Markdown file to figures/<name>.png.

A block is named by the HTML comment immediately preceding it:

    <!-- figure: v2_relay_architecture -->
    ```mermaid
    ...
    ```

Rendering uses the hosted mermaid.ink service (no local browser or Node needed).
Usage:  python render-mermaid.py ../../V2_FEATURE_REQUESTS.md [--width 1000]
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
import urllib.request
from pathlib import Path

BLOCK = re.compile(r"<!--\s*figure:\s*([\w.-]+)[^\n]*-->\s*\n```mermaid\n(.*?)```", re.S)


def render(code: str, width: int, scale: int) -> bytes:
    state = json.dumps({"code": code, "mermaid": {"theme": "neutral"}}).encode()
    token = base64.urlsafe_b64encode(state).decode().rstrip("=")
    url = f"https://mermaid.ink/img/{token}?type=png&width={width}&scale={scale}&bgColor=ffffff"
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (render-mermaid.py)"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("markdown", type=Path)
    parser.add_argument("--width", type=int, default=1000, help="layout width in CSS px")
    parser.add_argument("--scale", type=int, default=3, help="device scale factor")
    args = parser.parse_args()

    text = args.markdown.read_text(encoding="utf-8")
    figures = args.markdown.parent / "figures"
    figures.mkdir(exist_ok=True)
    found = 0
    for name, code in BLOCK.findall(text):
        target = figures / f"{name}.png"
        target.write_bytes(render(code, args.width, args.scale))
        print(f"{target} ({target.stat().st_size} bytes)")
        found += 1
    if not found:
        print("no named mermaid blocks found", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
