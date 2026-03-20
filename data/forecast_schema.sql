-- ================================================================
-- Stage 2 Schema  --  ForecastPredictions
-- ADDITIVE ONLY.  DrawHistory is untouched.
--
-- Mirrors SQL Server schema from Stage_Two_Methods.txt.
-- SQLite adaptations:
--   BIGINT IDENTITY  → INTEGER (SQLite autoincrement via rowid)
--   datetime2(0)     → TEXT (ISO-8601, YYYY-MM-DD HH:MM:SS)
--   GETDATE()        → (strftime('%Y-%m-%dT%H:%M:%S','now'))
--   dbo.ForecastBandSet TVP → not applicable in SQLite (Python loop)
-- ================================================================

CREATE TABLE IF NOT EXISTS ForecastPredictions (
    ForecastPredictionId  INTEGER  NOT NULL,
    LottoType             CHAR(2)  NOT NULL,
    DrawDate              DATE     NOT NULL,   -- TEXT 'YYYY-MM-DD'
    SetNumber             INTEGER  NOT NULL,   -- 1..6
    SafeLow               INTEGER  NOT NULL,
    SafeHigh              INTEGER  NOT NULL,
    HotLow                INTEGER  NULL,
    HotHigh               INTEGER  NULL,
    ModelVersion          TEXT     NOT NULL,   -- e.g. 'WF_v4_baseline'
    CreatedAt             TEXT     NOT NULL
                              DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now')),

    CONSTRAINT PK_ForecastPredictions
        PRIMARY KEY (ForecastPredictionId AUTOINCREMENT),

    -- Uniqueness mirrors sp_PersistForecastBands WHERE NOT EXISTS guard:
    -- (LottoType, DrawDate, SetNumber, ModelVersion) must be unique.
    CONSTRAINT UQ_ForecastPredictions
        UNIQUE (LottoType, DrawDate, SetNumber, ModelVersion)
);

-- Covering index for the viewer's primary query pattern:
-- SELECT * FROM ForecastPredictions
--   WHERE LottoType=? AND DrawDate BETWEEN ? AND ? AND ModelVersion=?
CREATE INDEX IF NOT EXISTS IX_FP_Type_Date_Version
    ON ForecastPredictions (LottoType, DrawDate, ModelVersion);