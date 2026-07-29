from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

from update_florida_results import parse_lottery_post, parse_lottery_valley


def test_parse_lottery_post_uses_main_draw_not_double_play():
    page = """
    <h2>Lotto</h2>
    <p>Saturday, July 25, 2026</p>
    <ul><li>7</li><li>26</li><li>34</li><li>39</li><li>46</li><li>52</li></ul>
    <p>Double Play Drawing</p>
    <ul><li>1</li><li>4</li><li>29</li><li>35</li><li>41</li><li>51</li></ul>
    <h2>Mega Millions</h2>
    """

    assert parse_lottery_post(page, 2026)[0]["draw_date"] == "2026-07-25"
    assert parse_lottery_post(page, 2026)[0]["n1"] == 7
    assert parse_lottery_post(page, 2026)[0]["n6"] == 52


def test_parse_lottery_valley_reads_lotto_table_only():
    page = """
    <h3>Florida Lotto Past Results &amp; Winning Numbers History (Last 30 Days)</h3>
    <table>
      <tr><th>Date</th><th>Winning numbers</th></tr>
      <tr><td>Sat, Jul 25, 2026</td><td>7 26 34 39 46 52</td></tr>
      <tr><td>Wed, Jul 22, 2026</td><td>3 14 22 32 35 40</td></tr>
    </table>
    <h3>Florida Lotto Double Play Past Results</h3>
    """

    draws = parse_lottery_valley(page, 2026)

    assert [draw["draw_date"] for draw in draws] == ["2026-07-22", "2026-07-25"]
    assert draws[-1]["n1"] == 7
    assert draws[-1]["n6"] == 52
