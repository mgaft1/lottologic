-- ================================================================
-- Stage 3 Schema  --  CandidateCombinations
-- ADDITIVE ONLY.  DrawHistory and ForecastPredictions are untouched.
--
-- Stores the output of SelectionEngine.select() for each
-- (LottoType, DrawDate, ModelVersion) triple.
-- ================================================================

CREATE TABLE IF NOT EXISTS CandidateCombinations (
    CombinationId     INTEGER  NOT NULL,   -- 1-based rank within draw
    LottoType         CHAR(2)  NOT NULL,
    DrawDate          DATE     NOT NULL,   -- TEXT 'YYYY-MM-DD'
    Nbr1              INTEGER  NOT NULL,
    Nbr2              INTEGER  NOT NULL,
    Nbr3              INTEGER  NOT NULL,
    Nbr4              INTEGER  NOT NULL,
    Nbr5              INTEGER  NOT NULL,
    Nbr6              INTEGER  NOT NULL,
    Score             INTEGER  NOT NULL,
    SelectionReason   TEXT     NOT NULL,
    ModelVersion      TEXT     NOT NULL,
    CreatedAt         TEXT     NOT NULL
                          DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now')),

    CONSTRAINT PK_CandidateCombinations
        PRIMARY KEY (LottoType, DrawDate, CombinationId, ModelVersion),

    CONSTRAINT UQ_CandidateCombinations_Numbers
        UNIQUE (LottoType, DrawDate, Nbr1, Nbr2, Nbr3, Nbr4, Nbr5, Nbr6, ModelVersion)
);

-- Primary query pattern: fetch all combos for a draw, ordered by rank
CREATE INDEX IF NOT EXISTS IX_CC_Type_Date_Version
    ON CandidateCombinations (LottoType, DrawDate, ModelVersion);
