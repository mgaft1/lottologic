from __future__ import annotations

import argparse
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from pypdf import PdfReader


ROW_PATTERN = re.compile(
    r"(?P<date>\d{2}/\d{2}/\d{2})\s+"
    r"(?P<n1>\d{1,2})\s*-\s*(?P<n2>\d{1,2})\s*-\s*"
    r"(?P<n3>\d{1,2})\s*-\s*(?P<n4>\d{1,2})\s*-\s*"
    r"(?P<n5>\d{1,2})\s*-\s*(?P<n6>\d{1,2})\s+LOTTO(?!\s+DP)"
)


def parse_draws(content: bytes, year: int) -> list[dict]:
    draws: dict[str, dict] = {}
    reader = PdfReader(io.BytesIO(content))
    for page in reader.pages:
        text = page.extract_text(extraction_mode="layout") or ""
        for match in ROW_PATTERN.finditer(text):
            draw_date = datetime.strptime(match.group("date"), "%m/%d/%y")
            if draw_date.year != year:
                continue
            date_key = draw_date.strftime("%Y-%m-%d")
            numbers = [int(match.group(f"n{i}")) for i in range(1, 7)]
            draws[date_key] = {
                "draw_date": date_key,
                **{f"n{i}": number for i, number in enumerate(numbers, start=1)},
            }
    return sorted(draws.values(), key=lambda row: row["draw_date"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("pdf", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--year", type=int, default=datetime.now().year)
    args = parser.parse_args()

    draws = parse_draws(args.pdf.read_bytes(), args.year)
    if not draws:
        raise SystemExit(f"No Florida Lotto draws found for {args.year}")

    if args.output.exists():
        try:
            existing = json.loads(args.output.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            existing = {}
        if existing.get("draws") == draws:
            print(f"Florida Lotto results are already current through {draws[-1]['draw_date']}")
            return

    payload = {
        "source": "https://files.floridalottery.com/exptkt/l6.pdf",
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "draws": draws,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Published {len(draws)} Florida Lotto draws through {draws[-1]['draw_date']}")


if __name__ == "__main__":
    main()
