-- ================================================================
-- Lotto Viewer -- Phase 1 Schema
-- SQLite now; forward-compatible with SQL Server (no SQLite tricks)
-- ================================================================

CREATE TABLE IF NOT EXISTS DrawHistory (
    Id          INTEGER       NOT NULL,
    LottoType   CHAR(2)       NOT NULL,
    DrawDate    DATE          NOT NULL,   -- stored as TEXT 'YYYY-MM-DD'
    DrawIndex   INTEGER       NOT NULL,
    Nbr1        INTEGER       NOT NULL,
    Nbr2        INTEGER       NOT NULL,
    Nbr3        INTEGER       NOT NULL,
    Nbr4        INTEGER       NOT NULL,
    Nbr5        INTEGER       NOT NULL,
    Nbr6        INTEGER       NULL,       -- bonus ball; NULL means not applicable
    CONSTRAINT PK_DrawHistory PRIMARY KEY (Id),
    CONSTRAINT UQ_DrawHistory UNIQUE (LottoType, DrawDate)
);

CREATE INDEX IF NOT EXISTS IX_DrawHistory_Type_Index
    ON DrawHistory (LottoType, DrawIndex);
