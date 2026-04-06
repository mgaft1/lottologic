from __future__ import annotations

"""
scraper.py  ──  Phase 1 missing-draw fetcher

Sources (verified against real HTML from each site):

  CA  https://www.lottery.net/california/superlotto-plus/numbers/YYYY
      Balls: <li class="ball"> x5 (sorted), <li class="mega-ball">
      Date:  from Prize Payout href  /numbers/MM-DD-YYYY

  MM  https://www.lottery.net/mega-millions/numbers/YYYY
      Balls: <li class="ball"> x5 (sorted), <li class="mega-ball">
      Date:  from Prize Payouts href  /numbers/MM-DD-YYYY
      Handles both div.latestResults and div.previousResults blocks

  FL  https://www.lottonumbers.com/florida-lotto/numbers/YYYY
      Balls: <li class="ball ball"> x6  (all main, no separate bonus)
      Date:  td.date-row  "Wed, Feb 18 2026"

  PB  https://www.coloradolottery.com/en/games/powerball/drawings/YYYY-MM/
  PD  (same page, Double Play section)
      Balls: <p class="draw"><span> x5, <p class="extra"><span> = bonus
      Date:  from href  /drawings/YYYY-MM-DD/
      Month-based pagination; all months listed in <select class="go-to-month">

Background thread: runs once at startup (current year only), then full history
every 6 hours. Any failure is caught, logged, silently ignored.
All inserts idempotent (INSERT OR IGNORE in db.insert_draw).
"""

import logging
import re
import threading
import time
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

import db

logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────
REQUEST_TIMEOUT  = 20
THROTTLE_SECS    = 2
SCRAPE_INTERVAL  = 6 * 3600   # seconds between full history passes
STAGGER_SECS     = 3           # delay between each lotto type scrape (3 seconds)

BROWSER_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/121.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


# ── HTTP fetch ───────────────────────────────────────────
def _fetch(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers=BROWSER_HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        logger.warning("Fetch failed %s: %s", url, exc)
        return None


# ── Parsers ──────────────────────────────────────────────

def parse_lottery_net_ca(html: str) -> list[dict]:
    """
    lottery.net CA SuperLotto Plus year page.
    Date from Prize Payout href: /california/superlotto-plus/numbers/MM-DD-YYYY
    Balls: li.ball x5 (sorted ascending), li.mega-ball = bonus.
    """
    soup = BeautifulSoup(html, 'lxml')
    return _parse_lottery_net_draws(
        soup,
        href_pattern=r'/california/superlotto-plus/numbers/\d{2}-\d{2}-\d{4}$',
    )


def parse_lottery_net_mm(html: str) -> list[dict]:
    """
    lottery.net Mega Millions year page.
    Handles both div.latestResults and div.previousResults blocks.
    Date from prize href: /mega-millions/numbers/MM-DD-YYYY
    Balls: li.ball x5 (sorted), li.mega-ball = bonus.
    """
    soup = BeautifulSoup(html, 'lxml')
    return _parse_lottery_net_draws(
        soup,
        href_pattern=r'/mega-millions/numbers/\d{2}-\d{2}-\d{4}$',
    )


def _parse_lottery_net_draws(soup: BeautifulSoup, href_pattern: str) -> list[dict]:
    """
    Parse Lottery.net year pages for CA SuperLotto and Mega Millions.

    Their markup has changed over time. Older pages grouped draws inside
    specific div containers; newer pages still include the dated result links
    and ball list items, but not necessarily the same wrapper classes.
    """
    pattern = re.compile(href_pattern)
    results = []
    seen_dates = set()

    for link in soup.find_all('a', href=pattern):
        href = link.get('href', '')
        m = re.search(r'/(\d{2}-\d{2}-\d{4})$', href)
        if not m:
            continue

        draw_date = datetime.strptime(m.group(1), '%m-%d-%Y').strftime('%Y-%m-%d')
        if draw_date in seen_dates:
            continue

        container = _find_lottery_net_result_container(link)
        if not container:
            continue

        parsed = _extract_lottery_net_numbers(container, draw_date)
        if parsed:
            results.append(parsed)
            seen_dates.add(draw_date)

    return results


def _find_lottery_net_result_container(link: Tag) -> Optional[Tag]:
    """
    Walk up from a dated result link until we reach a node that contains the
    corresponding ball list. This works across both the old and newer layouts.
    """
    for parent in link.parents:
        if not isinstance(parent, Tag):
            continue

        main_balls = parent.select('li.ball')
        bonus_ball = parent.select_one('li.mega-ball')
        if len(main_balls) >= 5 and bonus_ball:
            return parent

    return None


def _extract_lottery_net_numbers(container: Tag, draw_date: str) -> Optional[dict]:
    nums = sorted(
        int(ball.get_text(strip=True))
        for ball in container.select('li.ball')
        if ball.get_text(strip=True).isdigit()
    )
    mega = container.select_one('li.mega-ball')
    bonus = None
    if mega:
        mega_text = mega.get_text(strip=True)
        if mega_text.isdigit():
            bonus = int(mega_text)

    if len(nums) != 5 or bonus is None:
        return None

    return {
        'draw_date': draw_date,
        'n1': nums[0], 'n2': nums[1], 'n3': nums[2],
        'n4': nums[3], 'n5': nums[4], 'n6': bonus,
    }


def parse_lottonumbers_fl(html: str) -> list[dict]:
    """
    lottonumbers.com Florida Lotto year page.
    Date: td.date-row  "Wed, Feb 18 2026"
    Balls: li.ball x6 — all main numbers (FL has no separate bonus ball).
    """
    soup = BeautifulSoup(html, 'lxml')
    results = []
    for row in soup.select('table tbody tr'):
        date_td = row.select_one('td.date-row')
        balls_ul = row.select_one('ul.balls')
        if not date_td or not balls_ul:
            continue
        raw = date_td.text.strip()
        dt = None
        for fmt in ('%a, %b %d %Y', '%A, %B %d %Y', '%a, %B %d %Y'):
            try:
                dt = datetime.strptime(raw, fmt).strftime('%Y-%m-%d')
                break
            except ValueError:
                pass
        if not dt:
            continue
        nums = [int(li.text.strip()) for li in balls_ul.find_all('li')
                if li.text.strip().isdigit()]
        if len(nums) == 6:
            results.append({'draw_date': dt,
                            'n1': nums[0], 'n2': nums[1], 'n3': nums[2],
                            'n4': nums[3], 'n5': nums[4], 'n6': nums[5]})
    return results


def parse_colorado_pb_pd(html: str) -> tuple[list[dict], list[dict]]:
    """
    coloradolottery.com Powerball + Double Play month page.
    Date: from href /en/games/powerball/drawings/YYYY-MM-DD/
    PB:  first div.draw per drawing (title "Powerball Numbers")
         <p class="draw"><span> x5, <p class="extra"><span> = powerball
    PD:  second div.draw (title "Double Play Numbers"), same structure.
    Returns (pb_draws, pd_draws).
    """
    soup = BeautifulSoup(html, 'lxml')
    pb_draws, pd_draws = [], []

    for drawing in soup.select('div.drawing'):
        date_a = drawing.select_one('div.date a')
        if not date_a:
            continue
        m = re.search(r'/drawings/(\d{4}-\d{2}-\d{2})/', date_a.get('href', ''))
        if not m:
            continue
        date_str = m.group(1)   # YYYY-MM-DD directly

        for div in drawing.select('div.draws div.draw'):
            title_el = div.select_one('p.title')
            if not title_el:
                continue
            title = title_el.text.strip()

            nums_p  = div.select_one('div.numbers-and-jackpot p.draw')
            extra_p = div.select_one('div.numbers-and-jackpot p.extra')
            if not nums_p:
                continue

            nums  = [int(s.text.strip()) for s in nums_p.find_all('span')
                     if s.text.strip().isdigit()]
            bonus = None
            if extra_p:
                sp = extra_p.find('span')
                bonus = int(sp.text.strip()) if sp and sp.text.strip().isdigit() else None

            if len(nums) != 5:
                continue
            rec = {'draw_date': date_str, 'n1': nums[0], 'n2': nums[1],
                   'n3': nums[2], 'n4': nums[3], 'n5': nums[4], 'n6': bonus}
            if 'Powerball Numbers' in title:
                pb_draws.append(rec)
            elif 'Double Play' in title:
                pd_draws.append(rec)

    return pb_draws, pd_draws


def parse_powerball_previous_results(html: str, game_code: str) -> list[dict]:
    """
    Parse official Powerball previous-results pages.

    Supported game_code values:
      - "powerball"
      - "pb-double-play"

    We rely on the result card links because they contain both the draw date
    and the winning numbers in a compact, server-rendered format that has
    proven more stable than the Colorado Lottery page.
    """
    soup = BeautifulSoup(html, "lxml")
    draws = []
    seen_dates = set()

    href_re = re.compile(rf"/draw-result\?date=(\d{{4}}-\d{{2}}-\d{{2}})&gc={re.escape(game_code)}\b")

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        match = href_re.search(href)
        if not match:
            continue

        draw_date = match.group(1)
        if draw_date in seen_dates:
            continue

        text = " ".join(link.stripped_strings)
        nums = [int(n) for n in re.findall(r"\b\d+\b", text)]
        if len(nums) < 9:
            continue

        # The first three numbers are month/day/year from the human-readable
        # date; the next six are the winning numbers we want.
        balls = nums[3:9]
        if len(balls) != 6:
            continue

        draws.append({
            "draw_date": draw_date,
            "n1": balls[0],
            "n2": balls[1],
            "n3": balls[2],
            "n4": balls[3],
            "n5": balls[4],
            "n6": balls[5],
        })
        seen_dates.add(draw_date)

    return draws


def get_colorado_month_urls(html: str) -> list[str]:
    """
    Extract all month page URLs from the <select class="go-to-month"> dropdown.
    Returns list of full URLs sorted oldest-first.
    """
    soup = BeautifulSoup(html, 'lxml')
    base = 'https://www.coloradolottery.com'
    urls = []
    for opt in soup.select('select.go-to-month option'):
        val = opt.get('value', '')
        if re.match(r'/en/games/powerball/drawings/\d{4}-\d{2}/', val):
            urls.append(base + val)
    # Sort chronologically (URL contains YYYY-MM so lexicographic = chronological)
    return sorted(set(urls))


# ── Per-type scrapers ────────────────────────────────────

def _scrape_ca(existing: set[str]) -> int:
    base  = 'https://www.lottery.net/california/superlotto-plus/numbers/'
    start = 2000
    inserted = 0
    for year in range(start, datetime.now().year + 1):
        html = _fetch(f'{base}{year}')
        if not html:
            time.sleep(THROTTLE_SECS)
            continue
        draws = parse_lottery_net_ca(html)
        for d in draws:
            if d['draw_date'] not in existing:
                if db.insert_draw('CA', d['draw_date'],
                                  d['n1'], d['n2'], d['n3'], d['n4'], d['n5'], d['n6']):
                    inserted += 1
                    existing.add(d['draw_date'])
        time.sleep(THROTTLE_SECS)
    return inserted


def _scrape_mm(existing: set[str]) -> int:
    base  = 'https://www.lottery.net/mega-millions/numbers/'
    start = 2002
    inserted = 0
    for year in range(start, datetime.now().year + 1):
        html = _fetch(f'{base}{year}')
        if not html:
            time.sleep(THROTTLE_SECS)
            continue
        draws = parse_lottery_net_mm(html)
        for d in draws:
            if d['draw_date'] not in existing:
                if db.insert_draw('MM', d['draw_date'],
                                  d['n1'], d['n2'], d['n3'], d['n4'], d['n5'], d['n6']):
                    inserted += 1
                    existing.add(d['draw_date'])
        time.sleep(THROTTLE_SECS)
    return inserted


def _scrape_fl(existing: set[str]) -> int:
    base  = 'https://www.lottonumbers.com/florida-lotto/numbers/'
    start = 1988
    inserted = 0
    for year in range(start, datetime.now().year + 1):
        html = _fetch(f'{base}{year}')
        if not html:
            time.sleep(THROTTLE_SECS)
            continue
        draws = parse_lottonumbers_fl(html)
        for d in draws:
            if d['draw_date'] not in existing:
                if db.insert_draw('FL', d['draw_date'],
                                  d['n1'], d['n2'], d['n3'], d['n4'], d['n5'], d['n6']):
                    inserted += 1
                    existing.add(d['draw_date'])
        time.sleep(THROTTLE_SECS)
    return inserted


def _scrape_pb_pd(existing_pb: set[str], existing_pd: set[str]) -> tuple[int, int]:
    """
    Prefer official Powerball result pages for recent PB/PD results because
    they are more stable on Render. Fall back to the older Colorado Lottery
    month scraper if the official pages produce no parsable rows.
    """
    inserted_pb = inserted_pd = 0

    official_sources = [
        ("PB", "https://www.powerball.com/previous-results", "powerball", existing_pb),
        ("PD", "https://www.powerball.com/previous-results?gc=pb-double-play", "pb-double-play", existing_pd),
    ]

    parsed_any = False
    for lotto_type, url, game_code, existing in official_sources:
        html = _fetch(url)
        if not html:
            logger.warning("%s official previous-results fetch returned no HTML", lotto_type)
            continue

        draws = parse_powerball_previous_results(html, game_code)
        logger.info("%s official previous-results parsed %d draw(s)", lotto_type, len(draws))
        if draws:
            parsed_any = True

        for d in draws:
            if d["draw_date"] in existing:
                continue
            if db.insert_draw(
                lotto_type,
                d["draw_date"],
                d["n1"], d["n2"], d["n3"], d["n4"], d["n5"], d["n6"],
            ):
                if lotto_type == "PB":
                    inserted_pb += 1
                else:
                    inserted_pd += 1
                existing.add(d["draw_date"])

    if parsed_any:
        return inserted_pb, inserted_pd

    logger.warning("Official PB/PD source produced no rows; falling back to Colorado Lottery source.")

    base_url   = 'https://www.coloradolottery.com/en/games/powerball/drawings/'
    now        = datetime.now()
    first_page = f'{base_url}{now.year}-{now.month:02d}/'
    html = _fetch(first_page)
    if not html:
        return inserted_pb, inserted_pd

    month_urls = get_colorado_month_urls(html)
    if not month_urls:
        # Fallback: just use this month
        month_urls = [first_page]

    for url in month_urls:
        page_html = html if url == first_page else _fetch(url)
        if not page_html:
            time.sleep(THROTTLE_SECS)
            continue
        pb_draws, pd_draws = parse_colorado_pb_pd(page_html)

        for d in pb_draws:
            if d['draw_date'] not in existing_pb:
                if db.insert_draw('PB', d['draw_date'],
                                  d['n1'], d['n2'], d['n3'], d['n4'], d['n5'], d['n6']):
                    inserted_pb += 1
                    existing_pb.add(d['draw_date'])

        for d in pd_draws:
            if d['draw_date'] not in existing_pd:
                if db.insert_draw('PD', d['draw_date'],
                                  d['n1'], d['n2'], d['n3'], d['n4'], d['n5'], d['n6']):
                    inserted_pd += 1
                    existing_pd.add(d['draw_date'])

        time.sleep(THROTTLE_SECS)

    return inserted_pb, inserted_pd


# ── Full pass ────────────────────────────────────────────

def run_scrape_pass(current_year_only: bool = False) -> dict:
    """
    Scrape all types. Returns {type: count}. Never raises.
    current_year_only=True fetches only the current year page for CA/MM/FL
    and only the current month page for PB/PD (fast startup pass).
    """
    summary = {lt: 0 for lt in ['CA', 'FL', 'MM', 'PB', 'PD']}

    def _run(name, fn, *args):
        try:
            n = fn(*args)
            if isinstance(n, tuple):
                summary['PB'], summary['PD'] = n
            else:
                summary[name] = n
            if n and (not isinstance(n, tuple) or any(n)):
                logger.info("%s: inserted %s new draw(s)", name, n)
        except Exception as exc:
            logger.warning("%s scrape error: %s", name, exc)

    def _stagger(label):
        # Wait between lotto type scrapes to reduce server load
        if not _stop_event.is_set():
            logger.info("Waiting %ss before scraping %s...", STAGGER_SECS, label)
            _stop_event.wait(timeout=STAGGER_SECS)

    if current_year_only:
        # Fast pass: current year/month only
        _run('CA', lambda: _scrape_year_only(
            'CA', 'https://www.lottery.net/california/superlotto-plus/numbers/',
            parse_lottery_net_ca, db.get_existing_dates('CA')))
        _stagger('MM')
        _run('MM', lambda: _scrape_year_only(
            'MM', 'https://www.lottery.net/mega-millions/numbers/',
            parse_lottery_net_mm, db.get_existing_dates('MM')))
        _stagger('FL')
        _run('FL', lambda: _scrape_year_only(
            'FL', 'https://www.lottonumbers.com/florida-lotto/numbers/',
            parse_lottonumbers_fl, db.get_existing_dates('FL')))
        _stagger('PB/PD')
        _run('PB_PD', _scrape_pb_pd,
             db.get_existing_dates('PB'), db.get_existing_dates('PD'))
    else:
        _run('CA', _scrape_ca, db.get_existing_dates('CA'))
        _stagger('MM')
        _run('MM', _scrape_mm, db.get_existing_dates('MM'))
        _stagger('FL')
        _run('FL', _scrape_fl, db.get_existing_dates('FL'))
        _stagger('PB/PD')
        _run('PB_PD', _scrape_pb_pd,
             db.get_existing_dates('PB'), db.get_existing_dates('PD'))

    return summary


def _scrape_year_only(lotto_type: str, base: str, parser, existing: set[str]) -> int:
    """Fetch only the current year page for CA/MM/FL."""
    year = datetime.now().year
    html = _fetch(f'{base}{year}')
    if not html:
        return 0
    draws = parser(html)
    inserted = 0
    for d in draws:
        if d['draw_date'] not in existing:
            if db.insert_draw(lotto_type, d['draw_date'],
                              d['n1'], d['n2'], d['n3'], d['n4'], d['n5'], d['n6']):
                inserted += 1
                existing.add(d['draw_date'])
    return inserted


# ── Background worker ────────────────────────────────────

_stop_event = threading.Event()
_scraper_thread = None


def _worker():
    logger.info("Scraper worker started")
    first = True
    while not _stop_event.is_set():
        try:
            summary = run_scrape_pass(current_year_only=first)
            logger.info("Scrape pass (%s) complete: %s",
                        "current year" if first else "full history", summary)
            first = False
        except Exception as exc:
            logger.warning("Scrape pass error: %s", exc)
        for _ in range(SCRAPE_INTERVAL // 60):
            if _stop_event.wait(timeout=60):
                return


def refresh_lotto_type(lotto_type: str) -> dict[str, int]:
    """
    Run an on-demand incremental refresh for a single lotto type.

    Designed for request-time stale-data repair: fetch only the current
    year/month or recent official results, not a full historical pass.
    """
    lotto_type = lotto_type.upper()

    if lotto_type == "CA":
        inserted = _scrape_year_only(
            "CA",
            "https://www.lottery.net/california/superlotto-plus/numbers/",
            parse_lottery_net_ca,
            db.get_existing_dates("CA"),
        )
        return {"CA": inserted}

    if lotto_type == "MM":
        inserted = _scrape_year_only(
            "MM",
            "https://www.lottery.net/mega-millions/numbers/",
            parse_lottery_net_mm,
            db.get_existing_dates("MM"),
        )
        return {"MM": inserted}

    if lotto_type == "FL":
        inserted = _scrape_year_only(
            "FL",
            "https://www.lottonumbers.com/florida-lotto/numbers/",
            parse_lottonumbers_fl,
            db.get_existing_dates("FL"),
        )
        return {"FL": inserted}

    if lotto_type in {"PB", "PD"}:
        pb_inserted, pd_inserted = _scrape_pb_pd(
            db.get_existing_dates("PB"),
            db.get_existing_dates("PD"),
        )
        return {"PB": pb_inserted, "PD": pd_inserted}

    raise ValueError(f"Unsupported lotto type: {lotto_type}")


def start_background_scraper():
    """Launch daemon scraper thread. Returns immediately."""
    global _scraper_thread
    if _scraper_thread and _scraper_thread.is_alive():
        logger.info("Background scraper already running")
        return
    _stop_event.clear()
    _scraper_thread = threading.Thread(target=_worker, name='lotto-scraper', daemon=True)
    _scraper_thread.start()
    logger.info("Background scraper started")


def stop_background_scraper():
    _stop_event.set()
