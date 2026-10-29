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
    "lottery-net": "https://www.lottery.net/california/superlotto-plus/numbers/{year}",
    "lotto-numbers": "https://california.lottonumbers.com/superlotto-plus/past-numbers/{year}",
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
    fragment = re.sub(
        r"<(?:script|style)\b.*?</(?:script|style)>",
        " ",
        fragment,
        flags=re.IGNORECASE | re.DOTALL,
    )
    fragment = re.sub(r"<[^>]+>", "\n", fragment)
    return html.unescape(fragment).replace("\xa0", " ")


def _validated_draw(date_text: str, numbers: list[int], year: int) -> dict | None:
    draw_date = None
    for fmt in (
        "%m/%d/%Y",
        "%B %d, %Y",
        "%A, %B %d, %Y",
        "%Y-%m-%d",
    ):
        try:
            draw_date = datetime.strptime(" ".join(date_text.split()), fmt)
            break
        except ValueError:
            pass
    main = numbers[:5]
    if (
        draw_date is None
        or draw_date.year != year
        or draw_date.weekday() not in {2, 5}
        or len(numbers) != 6
        or main != sorted(main)
        or len(set(main)) != 5
        or any(number < 1 or number > 47 for number in main)
        or numbers[5] < 1
        or numbers[5] > 27
    ):
        return None
    return {
        "draw_date": draw_date.strftime("%Y-%m-%d"),
        **{f"n{i}": number for i, number in enumerate(numbers, start=1)},
    }


def _row_numbers(row_html: str) -> list[int]:
    return [
        int(value)
        for value in re.findall(
            r"<li\b[^>]*>\s*(\d{1,2})\s*</li>",
            row_html,
            flags=re.IGNORECASE | re.DOTALL,
        )[:6]
    ]


def _parse_text_draws(page: str, year: int, date_formats: tuple[str, ...]) -> list[dict]:
    """Fallback for archive pages whose result rows are divs rather than tables."""
    lines = [line.strip() for line in _plain_text(page).splitlines() if line.strip()]
    draws: dict[str, dict] = {}
    for index, line in enumerate(lines):
        parsed_date = None
        for date_format in date_formats:
            try:
                parsed_date = datetime.strptime(line, date_format)
                break
            except ValueError:
                pass
        if parsed_date is None or parsed_date.year != year:
            continue
        numbers: list[int] = []
        for candidate in lines[index + 1:index + 20]:
            if re.fullmatch(r"\d{1,2}", candidate):
                numbers.append(int(candidate))
                if len(numbers) == 6:
                    break
            elif numbers:
                break
        draw = _validated_draw(line, numbers, year)
        if draw:
            draws[draw["draw_date"]] = draw
    return sorted(draws.values(), key=lambda row: row["draw_date"])


def parse_lotto_numbers(page: str, year: int) -> list[dict]:
    """Parse the California LottoNumbers current-year archive table."""
    draws: dict[str, dict] = {}
    for row_html in re.findall(
        r"<tr\b[^>]*>(.*?)</tr>", page, flags=re.IGNORECASE | re.DOTALL
    ):
        date_match = re.search(
            r"(?<!\d)(\d{2}/\d{2}/\d{4})(?!\d)",
            _plain_text(row_html),
        )
        if not date_match:
            continue
        draw = _validated_draw(date_match.group(1), _row_numbers(row_html), year)
        if draw:
            draws[draw["draw_date"]] = draw
    for draw in _parse_text_draws(page, year, ("%m/%d/%Y",)):
        draws.setdefault(draw["draw_date"], draw)
    return sorted(draws.values(), key=lambda row: row["draw_date"])


def parse_lottery_net(page: str, year: int) -> list[dict]:
    """Parse the Lottery.net SuperLotto Plus current-year results table."""
    draws: dict[str, dict] = {}
    for row_html in re.findall(
        r"<tr\b[^>]*>(.*?)</tr>", page, flags=re.IGNORECASE | re.DOTALL
    ):
        date_match = re.search(
            r"(?:January|February|March|April|May|June|July|August|September|"
            r"October|November|December)\s+\d{1,2},\s+\d{4}",
            _plain_text(row_html),
            flags=re.IGNORECASE,
        )
        if not date_match:
            href_date = re.search(
                r"/numbers/(\d{2})-(\d{2})-(\d{4})(?:[\"'/#?]|$)",
                row_html,
                flags=re.IGNORECASE,
            )
            if not href_date:
                continue
            date_text = f"{href_date.group(1)}/{href_date.group(2)}/{href_date.group(3)}"
        else:
            date_text = date_match.group(0)
        draw = _validated_draw(date_text, _row_numbers(row_html), year)
        if draw:
            draws[draw["draw_date"]] = draw
    for draw in _parse_text_draws(page, year, ("%B %d, %Y",)):
        draws.setdefault(draw["draw_date"], draw)
    return sorted(draws.values(), key=lambda row: row["draw_date"])


PARSERS = {
    "lottery-net": parse_lottery_net,
    "lotto-numbers": parse_lotto_numbers,
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
        print(f"WARNING: {name} returned no validated California draws")
    return draws


def _numbers(row: dict) -> tuple[int, ...]:
    return tuple(int(row[f"n{i}"]) for i in range(1, 7))


def load_existing(path: Path, year: int) -> dict[str, dict]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    draws: dict[str, dict] = {}
    for row in payload.get("draws", []):
        try:
            draw = _validated_draw(str(row["draw_date"]), list(_numbers(row)), year)
        except (KeyError, TypeError, ValueError):
            draw = None
        if draw:
            draws[draw["draw_date"]] = draw
    return draws


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--year", type=int, default=datetime.now().year)
    args = parser.parse_args()

    existing = load_existing(args.output, args.year)
    votes: dict[str, Counter[tuple[int, ...]]] = defaultdict(Counter)
    source_urls = {name: url.format(year=args.year) for name, url in SOURCES.items()}
    available_sources = 0
    for name, url in source_urls.items():
        draws = fetch_source(name, url, args.year)
        if not draws:
            continue
        available_sources += 1
        for row in draws:
            votes[row["draw_date"]][_numbers(row)] += 1

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

    if available_sources < 2:
        print("WARNING: fewer than two sources were available; existing snapshot preserved")

    draws = sorted(accepted.values(), key=lambda row: row["draw_date"])
    if not draws:
        raise SystemExit("No trusted California results are available")
    age = (
        datetime.now(timezone.utc).date()
        - datetime.strptime(draws[-1]["draw_date"], "%Y-%m-%d").date()
    ).days
    if age > 8:
        raise SystemExit(
            f"California snapshot is stale: latest trusted draw is "
            f"{draws[-1]['draw_date']} ({age} days old)"
        )
    if accepted == existing:
        print(f"California snapshot is already current through {draws[-1]['draw_date']}")
        return

    payload = {
        "sources": list(source_urls.values()),
        "validation": "published only when two independent sources agree",
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "draws": draws,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Published {len(draws)} trusted California draws through {draws[-1]['draw_date']}")


if __name__ == "__main__":
    main()
