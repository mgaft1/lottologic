"""
selection.py  --  Stage 3 Selection Engine

Selects and ranks candidate number combinations using Stage 2 SAFE/HOT bands.
Answers: "Given SAFE and HOT bands, which concrete combinations should be played?"

Public surface
--------------
    BandSet              -- one draw's six band descriptors (input to engine)
    Combination          -- one ranked candidate combination (output)
    SelectionEngine      -- stateless engine; select() is the sole entry point
    select_for_draw      -- convenience wrapper: query DB + select in one call

Design decisions (all explicit and testable)
--------------------------------------------
Scoring formula  (per position):
    proximity_score(v, band) = floor(10 * (1 - |v - center| / safe_span))
                                clamped to [0, 10]
    hot_bonus(v, band)       = HOT_BONUS (10) if HotLow <= v <= HotHigh else 0
    position_score(v, band)  = proximity_score + hot_bonus   range [0, 20]
    combo_score              = sum of position_score for all positions

Reduction strategy:
    1. For each band position, score every value in [SafeLow, SafeHigh].
    2. Keep top TOP_K_PER_SET values by position_score (ties broken by value asc).
    3. Enumerate all strictly-increasing sorted combinations from those pools.
    4. Score each combination.
    5. Return top TOP_N by score descending (ties broken by combo values asc).

Game-type awareness:
    FL: 6 main balls, all strictly sorted (no separate bonus ball).
    CA / MM / PB / PD: 5 sorted main balls + 1 independent bonus ball.
    The engine reads GAME_TYPE from the lotto_type parameter.

Hard constraints honoured:
    - No randomness.
    - No ML.
    - No probability claims.
    - Stage 2 bands are never modified.
    - Viewer/UI is untouched.
    - Output is deterministic for identical input.
"""

import math
from dataclasses import dataclass, field
from itertools import combinations
from typing import Optional, List, Dict, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HOT_BONUS: int = 10         # added to position score when value is in HOT band
PROXIMITY_MAX: int = 10     # maximum proximity contribution per position
TOP_K_PER_SET: int = 6      # top candidate values to keep per band position
TOP_N: int = 50             # maximum combinations to return

# Lotto types whose set-6 is an independent bonus ball (not sorted with 1-5)
BONUS_BALL_TYPES: frozenset[str] = frozenset({"CA", "MM", "PB", "PD"})


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BandSet:
    """
    One band position's SAFE/HOT descriptor, plus the pre-computed center.

    set_number : 1-6
    safe_low   : inclusive lower bound (SAFE)
    safe_high  : inclusive upper bound (SAFE)
    hot_low    : inclusive lower bound (HOT)
    hot_high   : inclusive upper bound (HOT)
    center     : arithmetic mean of the lookback window for this position
                 (stored as float; used for proximity scoring)
    """
    set_number: int
    safe_low:   int
    safe_high:  int
    hot_low:    int
    hot_high:   int
    center:     float


@dataclass(frozen=True)
class Combination:
    """
    One ranked candidate combination.

    lotto_type       : 'CA', 'FL', 'MM', 'PB', 'PD'
    draw_date        : 'YYYY-MM-DD' the forecast is for
    combination_id   : 1-based rank within this draw
    nbr1..nbr6       : the six numbers (nbr1-5 strictly increasing; nbr6 independent for bonus types)
    score            : integer composite score (higher = better)
    selection_reason : human-readable label describing why this combo ranked here
    """
    lotto_type:       str
    draw_date:        str
    combination_id:   int
    nbr1:             int
    nbr2:             int
    nbr3:             int
    nbr4:             int
    nbr5:             int
    nbr6:             int
    score:            int
    selection_reason: str

    def as_dict(self) -> dict:
        return {
            "LottoType":       self.lotto_type,
            "DrawDate":        self.draw_date,
            "CombinationId":   self.combination_id,
            "Nbr1":            self.nbr1,
            "Nbr2":            self.nbr2,
            "Nbr3":            self.nbr3,
            "Nbr4":            self.nbr4,
            "Nbr5":            self.nbr5,
            "Nbr6":            self.nbr6,
            "Score":           self.score,
            "SelectionReason": self.selection_reason,
        }


# ---------------------------------------------------------------------------
# SelectionEngine
# ---------------------------------------------------------------------------

class SelectionEngine:
    """
    Stateless Stage 3 selection engine.

    Usage
    -----
        engine = SelectionEngine()
        combos = engine.select(lotto_type, draw_date, bands)

    Parameters to select()
    ----------------------
        lotto_type : str        -- 'CA', 'FL', 'MM', 'PB', 'PD'
        draw_date  : str        -- 'YYYY-MM-DD'
        bands      : List[BandSet]  -- exactly 6 BandSet objects, SetNumber 1-6
        top_n      : int        -- max combinations to return (default TOP_N)
        top_k      : int        -- candidates per position (default TOP_K_PER_SET)
    """

    def __init__(
        self,
        hot_bonus:     int = HOT_BONUS,
        proximity_max: int = PROXIMITY_MAX,
    ) -> None:
        self._hot_bonus     = hot_bonus
        self._proximity_max = proximity_max

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def select(
        self,
        lotto_type: str,
        draw_date:  str,
        bands:      list[BandSet],
        top_n:      int = TOP_N,
        top_k:      int = TOP_K_PER_SET,
    ) -> List[Combination]:
        """
        Return up to top_n ranked Combination objects for the given draw.

        Raises ValueError if bands does not contain exactly 6 BandSets
        with SetNumbers 1-6.
        """
        self._validate_bands(bands)
        band_map = {b.set_number: b for b in bands}

        is_bonus_type = lotto_type in BONUS_BALL_TYPES

        # Step 1: score and rank candidate values per position
        top_values = {}     # set_number -> List[int] (top_k values, score-ranked)
        value_scores = {}   # (set_number, value) -> int

        for s in range(1, 7):
            b = band_map[s]
            scored = self._score_position_values(b)
            # Keep top_k, ties broken by value ascending (deterministic)
            top_vals = sorted(
                scored.keys(),
                key=lambda v: (-scored[v], v)
            )[:top_k]
            top_values[s] = top_vals
            for v in top_vals:
                value_scores[(s, v)] = scored[v]

        # Step 2: enumerate valid sorted combinations
        if is_bonus_type:
            raw_combos = self._enumerate_bonus_type(top_values, value_scores)
        else:
            raw_combos = self._enumerate_all_main(top_values, value_scores)

        # Step 3: sort by score desc, then by values asc (deterministic tiebreak)
        raw_combos.sort(key=lambda x: (-x[0], x[1]))

        # Step 4: take top_n and build Combination objects
        result = []
        for rank, (score, nums) in enumerate(raw_combos[:top_n], start=1):
            hot_count = self._count_hot_hits(nums, band_map)
            reason = self._selection_reason(score, hot_count)
            result.append(Combination(
                lotto_type       = lotto_type,
                draw_date        = draw_date,
                combination_id   = rank,
                nbr1             = nums[0],
                nbr2             = nums[1],
                nbr3             = nums[2],
                nbr4             = nums[3],
                nbr5             = nums[4],
                nbr6             = nums[5],
                score            = score,
                selection_reason = reason,
            ))
        return result

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def position_score(self, value: int, band: BandSet) -> int:
        """
        Score a single value against a single band position.

        proximity = floor(PROXIMITY_MAX * (1 - |value - center| / safe_span))
                    clamped to [0, PROXIMITY_MAX]
        hot_bonus = HOT_BONUS if value in [HotLow, HotHigh] else 0
        total     = proximity + hot_bonus     range [0, PROXIMITY_MAX + HOT_BONUS]
        """
        safe_span = max(band.safe_high - band.safe_low, 1)
        proximity = math.floor(
            self._proximity_max * (1.0 - abs(value - band.center) / safe_span)
        )
        proximity = max(proximity, 0)
        hot = self._hot_bonus if band.hot_low <= value <= band.hot_high else 0
        return proximity + hot

    def _score_position_values(self, band: BandSet) -> Dict[int, int]:
        """Return {value: score} for every value in [SafeLow, SafeHigh]."""
        return {
            v: self.position_score(v, band)
            for v in range(band.safe_low, band.safe_high + 1)
        }

    # ------------------------------------------------------------------
    # Combination enumeration
    # ------------------------------------------------------------------

    def _enumerate_bonus_type(
        self,
        top_values:   dict[int, list[int]],
        value_scores: Dict[tuple[int, int], int],
    ) -> List[tuple[int, tuple[int, ...]]]:
        """
        CA / MM / PB / PD: sets 1-5 strictly sorted main, set 6 independent bonus.
        Returns list of (score, (n1,n2,n3,n4,n5,n6)).
        """
        result = []
        main_sets = [top_values[s] for s in range(1, 6)]
        bonus_vals = top_values[6]

        for n1 in main_sets[0]:
            for n2 in main_sets[1]:
                if n2 <= n1:
                    continue
                for n3 in main_sets[2]:
                    if n3 <= n2:
                        continue
                    for n4 in main_sets[3]:
                        if n4 <= n3:
                            continue
                        for n5 in main_sets[4]:
                            if n5 <= n4:
                                continue
                            main_score = (
                                value_scores[(1, n1)]
                                + value_scores[(2, n2)]
                                + value_scores[(3, n3)]
                                + value_scores[(4, n4)]
                                + value_scores[(5, n5)]
                            )
                            for n6 in bonus_vals:
                                total = main_score + value_scores[(6, n6)]
                                result.append((total, (n1, n2, n3, n4, n5, n6)))
        return result

    def _enumerate_all_main(
        self,
        top_values:   dict[int, list[int]],
        value_scores: Dict[tuple[int, int], int],
    ) -> List[tuple[int, tuple[int, ...]]]:
        """
        FL: all 6 positions are main balls, strictly sorted together.
        Pools sets 1-6 values, then generates C(pool, 6) sorted combos.
        Each value retains its original position's score.
        Returns list of (score, (n1,n2,n3,n4,n5,n6)).
        """
        # Build a unified pool with per-value best score across all sets
        # (a value may appear in multiple set pools; keep highest score)
        pool_scores: Dict[int, int] = {}
        for s in range(1, 7):
            for v in top_values[s]:
                sc = value_scores[(s, v)]
                if v not in pool_scores or sc > pool_scores[v]:
                    pool_scores[v] = sc

        pool = sorted(pool_scores.keys())
        result = []
        for combo in combinations(pool, 6):
            score = sum(pool_scores[v] for v in combo)
            result.append((score, combo))
        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _count_hot_hits(
        self, nums: Tuple[int, ...], band_map: Dict[int, BandSet]
    ) -> int:
        """Count how many of the 6 numbers fall within their position's HOT band."""
        count = 0
        for i, v in enumerate(nums, start=1):
            b = band_map[i]
            if b.hot_low <= v <= b.hot_high:
                count += 1
        return count

    @staticmethod
    def _selection_reason(score: int, hot_count: int) -> str:
        """
        Human-readable label.  Three tiers based on HOT hit count.
        """
        if hot_count == 6:
            return f"ALL_HOT score={score}"
        if hot_count >= 4:
            return f"HIGH_HOT({hot_count}) score={score}"
        if hot_count >= 2:
            return f"PARTIAL_HOT({hot_count}) score={score}"
        return f"SAFE_ONLY score={score}"

    @staticmethod
    def _validate_bands(bands: List[BandSet]) -> None:
        if len(bands) != 6:
            raise ValueError(f"Expected 6 BandSets, got {len(bands)}")
        set_numbers = sorted(b.set_number for b in bands)
        if set_numbers != [1, 2, 3, 4, 5, 6]:
            raise ValueError(f"BandSets must have SetNumbers 1-6, got {set_numbers}")
        for b in bands:
            if b.safe_low > b.safe_high:
                raise ValueError(
                    f"SetNumber {b.set_number}: safe_low ({b.safe_low}) > safe_high ({b.safe_high})"
                )
            if b.hot_low > b.hot_high:
                raise ValueError(
                    f"SetNumber {b.set_number}: hot_low ({b.hot_low}) > hot_high ({b.hot_high})"
                )


# ---------------------------------------------------------------------------
# Convenience wrapper: query DB + select
# ---------------------------------------------------------------------------

def select_for_draw(
    lotto_type:    str,
    draw_date:     str,
    model_version: str,
    *,
    top_n: int = TOP_N,
    top_k: int = TOP_K_PER_SET,
    _dal=None,
) -> List[Combination]:
    """
    Load Stage 2 bands from the database and run Stage 3 selection.

    Parameters
    ----------
    lotto_type    : 'CA', 'FL', 'MM', 'PB', 'PD'
    draw_date     : 'YYYY-MM-DD'
    model_version : e.g. 'WF_v4_baseline'
    top_n         : max combos to return
    top_k         : candidate values per band position
    _dal          : injectable DAL module (default: db_forecast)

    Returns
    -------
    List of Combination objects, sorted by score descending.
    Empty list if no bands exist for this draw.
    """
    if _dal is None:
        import db_forecast as _dal  # noqa: F811

    rows = _dal.get_forecast_bands(lotto_type, draw_date, draw_date, model_version)
    if len(rows) != 6:
        return []

    # We need centers — recompute from DrawHistory via db_forecast
    # Center = arithmetic mean of the lookback window for this position.
    # We store it via a helper that computes it on the fly from history.
    centers = _compute_centers(lotto_type, draw_date, model_version, _dal)

    bands = []
    for row in rows:
        s = row["SetNumber"]
        bands.append(BandSet(
            set_number = s,
            safe_low   = row["SafeLow"],
            safe_high  = row["SafeHigh"],
            hot_low    = row["HotLow"],
            hot_high   = row["HotHigh"],
            center     = centers.get(s, (row["SafeLow"] + row["SafeHigh"]) / 2.0),
        ))

    engine = SelectionEngine()
    return engine.select(lotto_type, draw_date, bands, top_n=top_n, top_k=top_k)


def _compute_centers(
    lotto_type:    str,
    draw_date:     str,
    model_version: str,
    dal,
) -> Dict[int, float]:
    """
    Re-derive per-set centers from DrawHistory for the lookback window
    that would have been used when forecasting draw_date.

    Returns {set_number: center_float} for sets 1-6.
    Falls back to mid-band if history is unavailable.
    """
    LOOKBACK_N = 44

    as_of_index = dal.get_draw_index_before(lotto_type, draw_date)
    if as_of_index <= 0:
        return {}

    history = dal.load_all_history(lotto_type)
    ub = as_of_index
    lb = max(1, as_of_index - LOOKBACK_N)
    window = [r for r in history if lb <= r["DrawIndex"] <= ub]
    if not window:
        return {}

    centers = {}
    for s in range(1, 7):
        key = f"Nbr{s}"
        vals = [r[key] for r in window if r[key] is not None]
        if vals:
            centers[s] = sum(vals) / len(vals)
    return centers
