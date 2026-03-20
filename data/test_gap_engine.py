"""
test_gap_engine.py  --  Deterministic tests for gap_engine.py

Run:  python -m pytest test_gap_engine.py -v
  or: python test_gap_engine.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import gap_engine as ge

# ── Helpers ──────────────────────────────────────────────────────────────────

def make_draw(idx, n1, n2, n3, n4, n5, n6=None, date=None):
    return {
        "DrawIndex": idx,
        "DrawDate":  date or f"2020-01-{idx:02d}",
        "Nbr1": n1, "Nbr2": n2, "Nbr3": n3,
        "Nbr4": n4, "Nbr5": n5,
        "Nbr6": n6,
    }

# ── TestComputeGap ────────────────────────────────────────────────────────────

class TestComputeGap:
    def test_all_up(self):
        a = make_draw(1, 1,1,1,1,1)
        b = make_draw(2, 2,3,4,5,6)
        assert ge.compute_gap(a, b) == (1,1,1,1,1)

    def test_all_down(self):
        a = make_draw(1, 5,5,5,5,5)
        b = make_draw(2, 1,2,3,4,4)
        assert ge.compute_gap(a, b) == (-1,-1,-1,-1,-1)

    def test_mixed(self):
        a = make_draw(1, 3,3,3,3,3)
        b = make_draw(2, 4,2,3,5,1)
        assert ge.compute_gap(a, b) == (1,-1,0,1,-1)

    def test_flat(self):
        a = make_draw(1, 7,7,7,7,7)
        b = make_draw(2, 7,7,7,7,7)
        assert ge.compute_gap(a, b) == (0,0,0,0,0)


# ── TestMatchingPositions ─────────────────────────────────────────────────────

class TestMatchingPositions:
    def test_full_match(self):
        g = (1,1,1,1,1)
        assert ge.matching_positions(g, g) == [1,2,3,4,5]

    def test_no_match(self):
        assert ge.matching_positions((1,1,1,1,1), (-1,-1,-1,-1,-1)) == []

    def test_partial(self):
        result = ge.matching_positions((1,-1,0,1,-1), (1,-1,1,0,-1))
        assert result == [1,2,5]

    def test_three_match(self):
        result = ge.matching_positions((1,0,-1,1,0), (1,0,-1,-1,1))
        assert result == [1,2,3]


# ── TestMatchedIndexSets ──────────────────────────────────────────────────────

class TestMatchedIndexSets:
    def test_all_sets_when_all_match(self):
        result = ge.matched_index_sets({1,2,3,4,5})
        assert set(tuple(s) for s in result) == {(1,2,3),(1,2,4),(1,2,5),(2,3,4),(3,4,5)}

    def test_set_123_only(self):
        result = ge.matched_index_sets({1,2,3})
        assert (1,2,3) in [tuple(s) for s in result]
        assert (3,4,5) not in [tuple(s) for s in result]

    def test_empty(self):
        assert ge.matched_index_sets(set()) == []

    def test_two_positions_no_set(self):
        # Only {1,2} → no complete 3-element set
        assert ge.matched_index_sets({1,2}) == []


# ── TestFindMatches ───────────────────────────────────────────────────────────

def _make_draws_with_gap_pattern(base_draws, insert_at, pattern):
    """
    Build a draw list where draws[insert_at] has a specific sign-gap
    relative to draws[insert_at-1], by adjusting draws[insert_at].
    pattern: 5-tuple of +1/0/-1
    """
    import copy
    draws = copy.deepcopy(base_draws)
    prev = draws[insert_at - 1]
    curr = draws[insert_at]
    for p in range(1, 6):
        if pattern[p-1] == 1:
            curr[f"Nbr{p}"] = prev[f"Nbr{p}"] + 3
        elif pattern[p-1] == -1:
            curr[f"Nbr{p}"] = max(1, prev[f"Nbr{p}"] - 3)
        else:
            curr[f"Nbr{p}"] = prev[f"Nbr{p}"]
    return draws


class TestFindMatches:

    def _base_draws(self, n=20):
        """20 draws with arbitrary values — all gaps will be deterministic."""
        import random
        rng = random.Random(42)
        draws = []
        vals = [5, 10, 15, 20, 25]
        for i in range(1, n + 1):
            new_vals = [max(1, v + rng.randint(-4, 4)) for v in vals]
            draws.append(make_draw(i, *new_vals))
            vals = new_vals
        return draws

    def test_empty_draws(self):
        assert ge.find_matches([]) == []

    def test_single_draw(self):
        assert ge.find_matches([make_draw(1,1,2,3,4,5)]) == []

    def test_two_draws_no_history(self):
        draws = [make_draw(1,1,2,3,4,5), make_draw(2,2,3,4,5,6)]
        # Only one transition, nothing to compare against
        assert ge.find_matches(draws) == []

    def test_returns_at_most_3(self):
        draws = self._base_draws(50)
        matches = ge.find_matches(draws)
        assert len(matches) <= 3

    def test_four_gap_returns_only_one(self):
        """If a 4-gap match exists, return exactly 1 match."""
        draws = self._base_draws(30)
        # Force the pattern of the last transition to appear earlier
        curr_gap = ge.compute_gap(draws[-2], draws[-1])
        # Plant same 4-position pattern at position 5
        prev = draws[4]
        planted = dict(draws[5])
        for p in range(1, 5):  # positions 1-4 match, 5 won't
            if curr_gap[p-1] == 1:
                planted[f"Nbr{p}"] = prev[f"Nbr{p}"] + 2
            elif curr_gap[p-1] == -1:
                planted[f"Nbr{p}"] = max(1, prev[f"Nbr{p}"] - 2)
            else:
                planted[f"Nbr{p}"] = prev[f"Nbr{p}"]
        draws[5] = planted
        matches = ge.find_matches(draws)
        four_plus = [m for m in matches if m["match_strength"] >= 4]
        if four_plus:
            assert len(matches) == 1

    def test_match_strength_at_least_3(self):
        draws = self._base_draws(50)
        matches = ge.find_matches(draws)
        for m in matches:
            assert m["match_strength"] >= 3

    def test_result_fields_present(self):
        draws = self._base_draws(50)
        matches = ge.find_matches(draws)
        required = {
            "anchor_index", "anchor_date", "next_index", "next_date",
            "match_strength", "matched_positions", "matched_sets",
            "anchor_draw", "next_draw", "gap_vector", "current_gap", "next_delta",
        }
        for m in matches:
            assert required <= set(m.keys()), f"Missing keys: {required - set(m.keys())}"

    def test_deterministic(self):
        draws = self._base_draws(50)
        r1 = ge.find_matches(draws)
        r2 = ge.find_matches(draws)
        assert r1 == r2

    def test_anchor_index_before_current(self):
        draws = self._base_draws(50)
        matches = ge.find_matches(draws)
        current_idx = draws[-1]["DrawIndex"]
        for m in matches:
            assert m["anchor_index"] < current_idx

    def test_next_draw_follows_anchor(self):
        draws = self._base_draws(50)
        matches = ge.find_matches(draws)
        idx_map = {d["DrawIndex"]: d for d in draws}
        for m in matches:
            anchor_pos = next(i for i, d in enumerate(draws)
                              if d["DrawIndex"] == m["anchor_index"])
            next_draw = draws[anchor_pos + 1]
            assert next_draw["DrawIndex"] == m["next_index"]

    def test_next_delta_correct(self):
        draws = self._base_draws(50)
        matches = ge.find_matches(draws)
        for m in matches:
            for p in range(1, 6):
                expected = m["next_draw"][f"Nbr{p}"] - m["anchor_draw"][f"Nbr{p}"]
                assert m["next_delta"][f"Nbr{p}"] == expected

    def test_sorted_most_recent_first(self):
        draws = self._base_draws(50)
        matches = ge.find_matches(draws)
        for i in range(len(matches) - 1):
            assert matches[i]["anchor_index"] >= matches[i+1]["anchor_index"]

    def test_custom_current_index(self):
        draws = self._base_draws(50)
        # Use draw 25 as current
        mid = draws[24]["DrawIndex"]
        matches = ge.find_matches(draws, current_index=mid)
        for m in matches:
            assert m["anchor_index"] < mid


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import traceback
    suites = [
        TestComputeGap, TestMatchingPositions,
        TestMatchedIndexSets, TestFindMatches,
    ]
    passed = failed = 0
    for suite in suites:
        obj = suite()
        for name in [n for n in dir(obj) if n.startswith("test_")]:
            try:
                getattr(obj, name)()
                print(f"  PASS  {suite.__name__}.{name}")
                passed += 1
            except Exception as e:
                print(f"  FAIL  {suite.__name__}.{name}  →  {e}")
                traceback.print_exc()
                failed += 1
    print(f"\n{passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
