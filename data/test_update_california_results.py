from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

from update_california_results import parse_lottery_net, parse_lotto_numbers


EXPECTED = [{
    "draw_date": "2026-10-24",
    "n1": 1, "n2": 2, "n3": 24, "n4": 31, "n5": 34, "n6": 17,
}]


def test_parse_lotto_numbers_archive_row():
    page = """
    <table><tr>
      <td class="date-row">10/24/2026</td>
      <td><ul class="balls">
        <li class="ball">1</li><li class="ball">2</li>
        <li class="ball">24</li><li class="ball">31</li>
        <li class="ball">34</li><li class="mega-ball">17</li>
      </ul></td>
    </tr></table>
    """
    assert parse_lotto_numbers(page, 2026) == EXPECTED


def test_parse_lottery_net_archive_row():
    page = """
    <table><tr>
      <td><a href="/california/superlotto-plus/numbers/10-24-2026">
        October 24, 2026</a></td>
      <td><ul>
        <li class="ball">1</li><li class="ball">2</li>
        <li class="ball">24</li><li class="ball">31</li>
        <li class="ball">34</li><li class="mega-ball">17</li>
      </ul></td>
    </tr></table>
    """
    assert parse_lottery_net(page, 2026) == EXPECTED


def test_bonus_number_may_duplicate_a_main_number():
    page = """
    <table><tr><td class="date-row">09/30/2026</td><td><ul>
      <li>2</li><li>10</li><li>12</li><li>23</li><li>39</li><li>2</li>
    </ul></td></tr></table>
    """
    assert parse_lotto_numbers(page, 2026)[0]["n6"] == 2


def test_div_based_archive_falls_back_to_rendered_text():
    page = """
    <div class="result"><h3>October 24, 2026</h3>
      <span>1</span><span>2</span><span>24</span>
      <span>31</span><span>34</span><span>17</span>
      <strong>$8,000,000</strong>
    </div>
    """
    assert parse_lottery_net(page, 2026) == EXPECTED
