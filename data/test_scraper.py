from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "files"))

from scraper import _load_fl_published_draws, parse_florida_official_pdf


def test_parse_florida_official_pdf_ignores_double_play(monkeypatch):
    class FakePage:
        def extract_text(self, extraction_mode=None):
            assert extraction_mode == "layout"
            return (
                "07/22/26  3 - 14- 22- 32- 35 - 40  LOTTO   "
                "07/22/26  1 - 5- 8- 15- 16 - 46  LOTTO DP\n"
                "07/18/26  8 - 13- 26- 28- 32 - 48  LOTTO"
            )

    class FakeReader:
        def __init__(self, _stream):
            self.pages = [FakePage()]

    monkeypatch.setattr("scraper.PdfReader", FakeReader)

    draws = parse_florida_official_pdf(b"pdf", 2026)

    assert draws == [
        {
            "draw_date": "2026-07-18",
            "n1": 8,
            "n2": 13,
            "n3": 26,
            "n4": 28,
            "n5": 32,
            "n6": 48,
        },
        {
            "draw_date": "2026-07-22",
            "n1": 3,
            "n2": 14,
            "n3": 22,
            "n4": 32,
            "n5": 35,
            "n6": 40,
        },
    ]


def test_load_fl_published_draws():
    draws = _load_fl_published_draws(2026)

    assert draws[-1] == {
        "draw_date": "2026-07-22",
        "n1": 3,
        "n2": 14,
        "n3": 22,
        "n4": 32,
        "n5": 35,
        "n6": 40,
    }
