"""
forecast.py  --  Stage 2 Forecast Engine

Exact translation of the C# model defined in Stage_Two_Methods.txt.
No statistical reinterpretation. No alterations to the math.

Public surface:
    ForecastContext      -- window descriptor (mirrors C# sealed class)
    ForecastEngine       -- ComputeMovingCenter / ComputeSafeDelta /
                            ComputeHotDelta / ComputeWindowMinMax
    BackfillPredictions  -- sole orchestrator (per implementation decision)

DO NOT MODIFY the four Compute* methods without a corresponding change
to the C# reference and an update to the regression test constants.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Callable, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ForecastContext
# Mirrors:  public sealed class ForecastContext
# ---------------------------------------------------------------------------

@dataclass
class ForecastContext:
    """
    Window descriptor for one forecast target.

    Attributes
    ----------
    target_draw_index : int
        The draw being forecast.  History is NOT loaded for this draw.
    lower_bound : int
        Inclusive lower bound of the lookback window.
        = max(1, target_draw_index - lookback_n)
    upper_bound : int
        Inclusive upper bound of the lookback window.
        = target_draw_index - 1
    lookback_n : int
        Number of draws requested.  Actual window may be smaller near
        the start of history (lower_bound clamps at 1).
    all_history : list[dict]
        Full draw history for this lotto type.  Each dict must contain
        keys: DrawIndex (int), Nbr1..Nbr6 (int|None).
        Populated by the caller after construction (mirrors C# pattern).
    """
    target_draw_index: int
    lookback_n: int
    lower_bound: int = field(init=False)
    upper_bound: int = field(init=False)
    all_history: list[dict] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        # Mirrors C# constructor exactly:
        #   UpperBoundDrawIndex = targetDrawIndex - 1
        #   LowerBoundDrawIndex = Math.Max(1, targetDrawIndex - lookbackN)
        self.upper_bound = self.target_draw_index - 1
        self.lower_bound = max(1, self.target_draw_index - self.lookback_n)


# ---------------------------------------------------------------------------
# ForecastEngine
# Mirrors:  public class ForecastEngine  (methods only; no state beyond lookback_n)
# ---------------------------------------------------------------------------

class ForecastEngine:
    """
    Stateless forecast engine.  All methods are deterministic given the same
    ForecastContext and AllHistory.
    """

    LOOKBACK_N: int = 44   # canonical value from BackfillPredictions

    def __init__(self, lookback_n: int = LOOKBACK_N) -> None:
        self._lookback_n = lookback_n

    # ------------------------------------------------------------------
    # ProcessDrawingDate
    # Mirrors:  public ForecastContext ProcessDrawingDate(int targetDrawIndex)
    # ------------------------------------------------------------------

    def process_drawing_date(self, target_draw_index: int) -> ForecastContext:
        return ForecastContext(
            target_draw_index=target_draw_index,
            lookback_n=self._lookback_n,
        )

    # ------------------------------------------------------------------
    # _get_window  (internal helper, replaces repeated LINQ Where)
    # ------------------------------------------------------------------

    @staticmethod
    def _get_window(ctx: ForecastContext) -> list[dict]:
        return [
            r for r in ctx.all_history
            if ctx.lower_bound <= r["DrawIndex"] <= ctx.upper_bound
        ]

    @staticmethod
    def _get_set_value(row: dict, set_number: int) -> int:
        """
        Mirrors LottoHelper.GetValueFromSet(v, setNumber).
        Returns the integer value for the given set (1-6).
        Nbr6 may be None for FL; callers must not request set 6
        for draws where it is NULL (scraper/db guarantee prevents this
        for FL by storing the 6th main ball, not a bonus).
        """
        return row[f"Nbr{set_number}"]

    # ------------------------------------------------------------------
    # ComputeMovingCenter
    # Mirrors:  public double ComputeMovingCenter(ForecastContext ctx, int setNumber)
    # ------------------------------------------------------------------

    def compute_moving_center(self, ctx: ForecastContext, set_number: int) -> float:
        if not ctx.all_history:
            raise ValueError("AllHistory is not initialized")

        window = self._get_window(ctx)
        if not window:
            raise ValueError("Empty sliding window")

        values = [self._get_set_value(r, set_number) for r in window]
        return sum(values) / len(values)   # arithmetic mean, returns float

    # ------------------------------------------------------------------
    # ComputeSafeDelta
    # Mirrors:  public int ComputeSafeDelta(..., double percentile = 0.90)
    # ------------------------------------------------------------------

    def compute_safe_delta(
        self,
        ctx: ForecastContext,
        set_number: int,
        percentile: float = 0.90,
    ) -> int:
        if not ctx.all_history:
            raise ValueError("AllHistory is not initialized")

        window = self._get_window(ctx)
        if not window:
            raise ValueError("Empty sliding window")

        center = self.compute_moving_center(ctx, set_number)
        deviations = sorted(
            abs(self._get_set_value(r, set_number) - center) for r in window
        )

        # Mirrors: int idx = (int)Math.Floor(percentile * (deviations.Count - 1))
        #          return (int)Math.Ceiling(deviations[idx])
        idx = int(math.floor(percentile * (len(deviations) - 1)))
        return math.ceil(deviations[idx])

    # ------------------------------------------------------------------
    # ComputeHotDelta
    # Mirrors:  public int ComputeHotDelta(..., double percentile = 0.65)
    # ------------------------------------------------------------------

    def compute_hot_delta(
        self,
        ctx: ForecastContext,
        set_number: int,
        percentile: float = 0.65,
    ) -> int:
        if not ctx.all_history:
            raise ValueError("AllHistory is not initialized")

        window = self._get_window(ctx)
        if not window:
            raise ValueError("Empty sliding window")

        center = self.compute_moving_center(ctx, set_number)
        deviations = sorted(
            abs(self._get_set_value(r, set_number) - center) for r in window
        )

        idx = int(math.floor(percentile * (len(deviations) - 1)))
        return math.ceil(deviations[idx])

    # ------------------------------------------------------------------
    # ComputeWindowMinMax
    # Mirrors:  public (int min, int max) ComputeWindowMinMax(ForecastContext ctx, int setNbr)
    # ------------------------------------------------------------------

    def compute_window_min_max(
        self, ctx: ForecastContext, set_number: int
    ) -> tuple[int, int]:
        values = [
            self._get_set_value(r, set_number)
            for r in ctx.all_history
            if ctx.lower_bound <= r["DrawIndex"] <= ctx.upper_bound
        ]

        if not values:
            raise ValueError("Empty window")

        return (min(values), max(values))


# ---------------------------------------------------------------------------
# BackfillPredictions  (sole orchestrator)
#
# Mirrors:  public void BackfillPredictions(
#               string state, int daysBack, string modelVersion, trace tracer)
#
# Adaptation notes:
#   • daysBack replaced by draw_dates list — callers use db.get_draw_dates()
#     or db.get_draw_dates_after() to construct the list before calling.
#   • LoadAllHistoryOnce  → db.load_all_history(lotto_type)
#   • getDrawIndexBefore  → db.get_draw_index_before(lotto_type, date_str)
#   • PersistForecastBands → db.persist_forecast_bands(...)
#   • tracer              → optional callable(str) for progress reporting
# ---------------------------------------------------------------------------

def backfill_predictions(
    lotto_type: str,
    draw_dates: list[str],          # ordered list of 'YYYY-MM-DD' strings
    model_version: str,
    *,
    tracer: Optional[Callable[[str], None]] = None,
    _dal=None,                      # injectable DAL module; defaults to db_forecast
) -> int:
    """
    Compute and persist forecast bands for every date in draw_dates.

    Parameters
    ----------
    lotto_type    : 'CA', 'FL', 'MM', 'PB', 'PD'
    draw_dates    : ordered list of draw dates to forecast (ascending)
    model_version : arbitrary label stored in ForecastPredictions.ModelVersion
    tracer        : optional progress callback, receives a status string
    _dal          : DAL module override for testing (default: db_forecast)

    Returns
    -------
    Number of draw dates for which bands were persisted (skips already-done
    dates via sp_PersistForecastBands idempotency).
    """
    if _dal is None:
        import db_forecast as _dal   # default; injectable for tests

    LOOKBACK_DRAWS = ForecastEngine.LOOKBACK_N

    draw_dates = sorted(draw_dates)   # ascending — mirrors .OrderBy(d => d)
    total = len(draw_dates)
    persisted = 0

    # Load full history once — mirrors LoadAllHistoryOnce
    all_history = _dal.load_all_history(lotto_type)

    engine = ForecastEngine(LOOKBACK_DRAWS)

    for i, forecast_date in enumerate(draw_dates, start=1):
        if tracer:
            tracer(f"Backfill {lotto_type} {i}/{total} : {forecast_date}")

        # Mirrors getDrawIndexBefore(state, forecastDate)
        as_of_index = _dal.get_draw_index_before(lotto_type, forecast_date)
        if as_of_index <= 0:
            continue                  # mirrors: if (asOfDrawIndex <= 0) continue;

        ctx = engine.process_drawing_date(as_of_index)
        ctx.all_history = all_history

        # Arrays indexed 1..6; index 0 unused (mirrors new int[7])
        safe_min: list[int]        = [0] * 7
        safe_max: list[int]        = [0] * 7
        hot_min:  list[Optional[int]] = [None] * 7
        hot_max:  list[Optional[int]] = [None] * 7

        for set_number in range(1, 7):
            center    = engine.compute_moving_center(ctx, set_number)
            safe_delta = engine.compute_safe_delta(ctx, set_number)
            hot_delta  = engine.compute_hot_delta(ctx, set_number)

            s_low  = round(center - safe_delta)
            s_high = round(center + safe_delta)
            h_low  = round(center - hot_delta)
            h_high = round(center + hot_delta)

            # Clamp to observed window range — mirrors ComputeWindowMinMax clamping
            min_legal, max_legal = engine.compute_window_min_max(ctx, set_number)

            s_low  = max(s_low,  min_legal)
            s_high = min(s_high, max_legal)
            h_low  = max(h_low,  min_legal)
            h_high = min(h_high, max_legal)

            # Collapse inverted bands — mirrors: if (sLow > sHigh) sLow = sHigh
            if s_low > s_high:
                s_low = s_high
            if h_low > h_high:
                h_low = h_high

            safe_min[set_number] = s_low
            safe_max[set_number] = s_high
            hot_min[set_number]  = h_low
            hot_max[set_number]  = h_high

        _dal.persist_forecast_bands(
            lotto_type    = lotto_type,
            draw_date     = forecast_date,
            model_version = model_version,
            safe_min      = safe_min,
            safe_max      = safe_max,
            hot_min       = hot_min,
            hot_max       = hot_max,
        )
        persisted += 1

    if tracer:
        tracer(f"Backfill {lotto_type} complete — {persisted}/{total} dates processed")

    return persisted
