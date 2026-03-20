"""
gap_engine.py  --  Gap-Pattern Matching Engine

Scans DrawHistory for historical draws whose gap pattern (direction of
movement per position) matches the current draw's gap pattern.

Definitions
-----------
gap(draw_A, draw_B):
    For each position p in 1..5:
        +1  if draw_B.Nbr_p > draw_A.Nbr_p   (up)
         0  if draw_B.Nbr_p == draw_A.Nbr_p   (flat)
        -1  if draw_B.Nbr_p < draw_A.Nbr_p   (down)

index_sets:
    Subsets of positions used for gap comparison:
    (1,2,3), (1,2,4), (1,2,5), (2,3,4), (3,4,5)

match_strength:
    Number of positions (out of 5) whose gap direction matches.

4-gap match:  match_strength >= 4
3-gap match:  match_strength == 3

Selection rules (from spec):
    - Prefer 4-gap matches (rare); if any exist keep only ONE
    - Otherwise up to 3 three-gap matches
    - Hard cap: max 3 matches total
    - One historical anchor active at a time

Output per match
----------------
    anchor_index  : DrawIndex of the historical draw that started the matching gap
    anchor_date   : DrawDate of that draw
    next_index    : DrawIndex of the draw after anchor (where the gap led)
    next_date     : DrawDate of that draw
    match_strength: int (3, 4, or 5)
    matched_positions: list[int]  -- which positions matched
    matched_sets  : list[tuple]   -- which index sets fully matched
    anchor_draw   : dict          -- the anchor draw's numbers
    next_draw     : dict          -- the next draw's numbers (the "what happened next")
    gap_vector    : tuple[int]    -- the actual gap direction vector (5 values)
"""

from __future__ import annotations
from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INDEX_SETS = [
    (1, 2, 3),
    (1, 2, 4),
    (1, 2, 5),
    (2, 3, 4),
    (3, 4, 5),
]

MAX_MATCHES   = 3
POSITIONS     = (1, 2, 3, 4, 5)


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _sign(x: int) -> int:
    if x > 0: return  1
    if x < 0: return -1
    return 0


def compute_gap(draw_a: dict, draw_b: dict) -> tuple:
    """
    Return a 5-tuple of sign-gaps (+1 / 0 / -1) for positions 1-5.
    draw_a is the earlier draw, draw_b is the later draw.
    """
    return tuple(
        _sign(draw_b[f"Nbr{p}"] - draw_a[f"Nbr{p}"])
        for p in POSITIONS
    )


def matching_positions(gap_a: tuple, gap_b: tuple) -> list[int]:
    """Return sorted list of positions (1-based) where gap directions match."""
    return [p for p in POSITIONS if gap_a[p - 1] == gap_b[p - 1]]


def matched_index_sets(pos_set: set) -> list[tuple]:
    """Return which INDEX_SETS are fully covered by the matching positions."""
    return [s for s in INDEX_SETS if all(p in pos_set for p in s)]


# ---------------------------------------------------------------------------
# Main search function
# ---------------------------------------------------------------------------

def find_matches(
    draws: list[dict],
    current_index: Optional[int] = None,
) -> list[dict]:
    """
    Find historical gap matches against the most recent draw transition.

    Parameters
    ----------
    draws : list[dict]
        All draws for a lotto type, sorted by DrawIndex ASCENDING.
        Each dict must have: DrawIndex, DrawDate, Nbr1..Nbr5.
    current_index : int, optional
        DrawIndex to treat as "current". Defaults to the last draw.
        The gap is computed between draws[current_index - 1] and draws[current_index].

    Returns
    -------
    List of match dicts, sorted by match_strength DESC then anchor_index DESC,
    capped at MAX_MATCHES (3).
    4-gap matches: if any exist, return only ONE (the most recent).
    3-gap matches: return up to 3.
    """
    if len(draws) < 2:
        return []

    # Build index lookup
    idx_map = {d["DrawIndex"]: i for i, d in enumerate(draws)}

    # Determine current draw position
    if current_index is None:
        curr_pos = len(draws) - 1
    else:
        curr_pos = idx_map.get(current_index, len(draws) - 1)

    if curr_pos < 1:
        return []

    current_draw = draws[curr_pos]
    prev_draw    = draws[curr_pos - 1]
    curr_gap     = compute_gap(prev_draw, current_draw)

    # Scan all historical transitions BEFORE the current one
    four_gap_matches = []
    three_gap_matches = []

    for i in range(1, curr_pos):
        hist_draw = draws[i]
        hist_prev = draws[i - 1]
        hist_gap  = compute_gap(hist_prev, hist_draw)

        m_pos = matching_positions(curr_gap, hist_gap)
        strength = len(m_pos)

        if strength < 3:
            continue

        # "next draw" = the draw after hist_draw (where the pattern led)
        next_pos = i + 1
        if next_pos >= len(draws):
            continue  # no "what happened next" available — skip
        next_draw = draws[next_pos]

        m_sets = matched_index_sets(set(m_pos))

        record = {
            "anchor_index":       hist_draw["DrawIndex"],
            "anchor_date":        hist_draw["DrawDate"],
            "next_index":         next_draw["DrawIndex"],
            "next_date":          next_draw["DrawDate"],
            "match_strength":     strength,
            "matched_positions":  m_pos,
            "matched_sets":       [list(s) for s in m_sets],
            "anchor_draw":        {f"Nbr{p}": hist_draw[f"Nbr{p}"] for p in range(1, 7)
                                   if f"Nbr{p}" in hist_draw},
            "next_draw":          {f"Nbr{p}": next_draw[f"Nbr{p}"] for p in range(1, 7)
                                   if f"Nbr{p}" in next_draw},
            "gap_vector":         list(hist_gap),
            "current_gap":        list(curr_gap),
            # Movement from anchor to next (for overlay projection)
            "next_delta":         {
                f"Nbr{p}": next_draw[f"Nbr{p}"] - hist_draw[f"Nbr{p}"]
                for p in range(1, 6)
            },
            # 5 draws before anchor + anchor + next (for mini-chart)
            "hist_context": [
                {f"Nbr{p}": draws[j][f"Nbr{p}"] for p in range(1, 7)
                 if f"Nbr{p}" in draws[j]}
                | {"DrawDate": draws[j]["DrawDate"], "DrawIndex": draws[j]["DrawIndex"]}
                for j in range(max(0, i - 5), i + 2)  # 5 before anchor, anchor, next
            ],
        }

        if strength >= 4:
            four_gap_matches.append(record)
        else:
            three_gap_matches.append(record)

    # Apply selection rules
    if four_gap_matches:
        four_gap_matches.sort(key=lambda x: -x["anchor_index"])
        result = [four_gap_matches[0]]
    else:
        three_gap_matches.sort(key=lambda x: -x["anchor_index"])
        result = three_gap_matches[:MAX_MATCHES]

    # Attach current context (same for all matches)
    curr_context = [
        {f"Nbr{p}": draws[j][f"Nbr{p}"] for p in range(1, 7)
         if f"Nbr{p}" in draws[j]}
        | {"DrawDate": draws[j]["DrawDate"], "DrawIndex": draws[j]["DrawIndex"]}
        for j in range(max(0, curr_pos - 5), curr_pos + 1)
    ]
    for r in result:
        r["curr_context"] = curr_context

    return result
