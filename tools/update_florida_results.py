from __future__ import annotations

import argparse
import html
import json
import re
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


SOURCES = {
    "lottery-post": "https://www.lotterypost.com/results/fl",
    "lottery-valley": "https://www.lotteryvalley.com/florida/past-results",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/150.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


def _plain_text(fragment: str) -> str:
    fragment = re.sub(r"<(?:script|style)\b.*?</(?:script|style)>", " ", fragment,
                      flags=re.IGNORECASE | re.DOTALL)
    fragment = re.sub(r"<[^>]+>", "\n", fragment)
    return html.unescape(fragment).replace("\xa0", " ")


def _validated_draw(date_text: str, numbers: list[int], year: int) -> dict | None:
    draw_date = None
    for fmt in (
        "%A, %B %d, %Y",
        "%a, %b %d, %Y",
        "%a, %b %d %Y",
        "%Y-%m-%d",
    ):
        try:
            draw_date = datetime.strptime(" ".join(date_text.split()), fmt)
            break
        except ValueError:
            pass
    if (
        draw_date is None
        or draw_date.year != year
        or draw_date.weekday() not in {2, 5}
        or len(numbers) != 6
        or numbers != sorted(numbers)
        or len(set(numbers)) != 6
        or any(number < 1 or number > 53 for number in numbers)
    ):
        return None

    return {
        "draw_date": draw_date.strftime("%Y-%m-%d"),
        **{f"n{i}": number for i, number in enumerate(numbers, start=1)},
    }


def parse_lottery_post(page: str, year: int) -> list[dict]:
    section_match = re.search(
        r"<h2\b[^>]*>\s*Lotto\s*</h2>(.*?)(?=<h2\b|$)",
        page,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not section_match:
        return []

    text = _plain_text(section_match.group(1))
    main_draw = re.split(r"Double\s+Play\s+Drawing", text, maxsplit=1,
                         flags=re.IGNORECASE)[0]
    date_match = re.search(
        r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
        r"[A-Za-z]+\s+\d{1,2},\s+\d{4}",
        main_draw,
    )
    if not date_match:
        return []

    after_date = main_draw[date_match.end():]
    numbers = [int(value) for value in re.findall(r"(?<!\d)\d{1,2}(?!\d)", after_date)[:6]]
    draw = _validated_draw(date_match.group(0), numbers, year)
    return [draw] if draw else []


def parse_lottery_valley(page: str, year: int) -> list[dict]:
    section_match = re.search(
        r"<h3\b[^>]*>[^<]*Florida\s+Lotto\s+Past\s+Results.*?</h3>"
        r"(.*?)(?=<h3\b|$)",
        page,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not section_match:
        return []

    draws: dict[str, dict] = {}
    for row_html in re.findall(
        r"<tr\b[^>]*>(.*?)</tr>",
        section_match.group(1),
        flags=re.IGNORECASE | re.DOTALL,
    ):
        cells = re.findall(
            r"<td\b[^>]*>(.*?)</td>",
            row_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if len(cells) < 2:
            continue
        date_text = " ".join(_plain_text(cells[0]).split())
        numbers = [
            int(value)
            for value in re.findall(r"(?<!\d)\d{1,2}(?!\d)", _plain_text(cells[1]))
        ]
        draw = _validated_draw(date_text, numbers, year)
        if draw:
            draws[draw["draw_date"]] = draw
    return sorted(draws.values(), key=lambda row: row["draw_date"])


PARSERS = {
    "lottery-post": parse_lottery_post,
    "lottery-valley": parse_lottery_valley,
}


def fetch_source(name: str, url: str, year: int) -> list[dict]:
    request = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            page = response.read().decode(
                response.headers.get_content_charset() or "utf-8",
                errors="replace",
            )
    except Exception as exc:
        print(f"WARNING: {name} could not be downloaded: {exc}")
        return []

    draws = PARSERS[name](page, year)
    if draws:
        print(f"{name} returned {len(draws)} validated draw(s) through {draws[-1]['draw_date']}")
    else:
        print(f"WARNING: {name} returned no validated Florida Lotto draws")
    return draws


def _row_numbers(row: dict) -> tuple[int, ...]:
    return tuple(int(row[f"n{i}"]) for i in range(1, 7))


def load_existing(path: Path, year: int) -> dict[str, dict]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}

    draws: dict[str, dict] = {}
    for row in payload.get("draws", []):
        try:
            normalized = _validated_draw(
                str(row["draw_date"]),
                list(_row_numbers(row)),
                year,
            )
        except (KeyError, TypeError, ValueError):
            normalized = None
        if normalized:
            draws[normalized["draw_date"]] = normalized
    return draws


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--year", type=int, default=datetime.now().year)
    args = parser.parse_args()

    existing = load_existing(args.output, args.year)
    votes: dict[str, Counter[tuple[int, ...]]] = defaultdict(Counter)
    source_count = 0

    for name, url in SOURCES.items():
        draws = fetch_source(name, url, args.year)
        if not draws:
            continue
        source_count += 1
        for row in draws:
            votes[row["draw_date"]][_row_numbers(row)] += 1

    accepted = dict(existing)
    for draw_date, candidates in votes.items():
        numbers, agreement = candidates.most_common(1)[0]
        if agreement < 2:
            print(f"Skipping {draw_date}: result was not confirmed by two sources")
            continue
        accepted[draw_date] = {
            "draw_date": draw_date,
            **{f"n{i}": number for i, number in enumerate(numbers, start=1)},
        }

    if source_count < 2:
        print("WARNING: fewer than two sources were available; existing snapshot preserved")

    draws = sorted(accepted.values(), key=lambda row: row["draw_date"])
    if not draws:
        print("WARNING: no trusted Florida Lotto results available; nothing written")
        return
    if accepted == existing:
        print(f"Florida Lotto snapshot is already current through {draws[-1]['draw_date']}")
        return

    payload = {
        "sources": list(SOURCES.values()),
        "validation": "published only when two independent sources agree",
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "draws": draws,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Published {len(draws)} trusted Florida Lotto draws through {draws[-1]['draw_date']}")


if __name__ == "__main__":
    main()
