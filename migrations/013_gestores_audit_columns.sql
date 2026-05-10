-- Migration 013: Audit columns for the LLM agent pipeline
-- ============================================================================
-- gestores already has the structured extraction fields (numbers, JSONB,
-- narratives) from prior manual work. The agent adds:
--   - Source PDF traceability (doc_id_m, doc_id_m1, doc_id_m12) so any
--     extracted number can be traced back to the input documents.
--   - Cost tracking (cost_usd) to know what each fund costs to process and
--     surface anomalies (e.g., a fund whose cost spikes 10x suggests a
--     prompt regression or runaway token usage).
--   - Provenance (extractor_model, prompt_version) so we can identify which
--     model + prompt produced a row, useful when iterating on prompts.
--   - Judge results (judge_passed, judge_notes) for the validation pass.
--
-- All columns are NULL-default so existing rows (5 manual extractions) stay
-- valid without backfill.
-- ============================================================================

ALTER TABLE gestores
    ADD COLUMN IF NOT EXISTS doc_id_m         BIGINT,
    ADD COLUMN IF NOT EXISTS doc_id_m1        BIGINT,
    ADD COLUMN IF NOT EXISTS doc_id_m12       BIGINT,
    ADD COLUMN IF NOT EXISTS cost_usd         NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS extractor_model  TEXT,
    ADD COLUMN IF NOT EXISTS prompt_version   TEXT,
    ADD COLUMN IF NOT EXISTS judge_passed     BOOLEAN,
    ADD COLUMN IF NOT EXISTS judge_notes      TEXT;

-- Useful indexes for the agent's operational queries
CREATE INDEX IF NOT EXISTS idx_gestores_doc_m ON gestores(doc_id_m);

-- Find rows where the judge flagged something (operational dashboard query)
CREATE INDEX IF NOT EXISTS idx_gestores_judge_failed
    ON gestores(processado_em DESC) WHERE judge_passed = FALSE;

COMMENT ON COLUMN gestores.doc_id_m IS
    'doc_id of the M (latest) relatório used as primary source';
COMMENT ON COLUMN gestores.doc_id_m1 IS
    'doc_id of the M-1 relatório, used for trend context';
COMMENT ON COLUMN gestores.doc_id_m12 IS
    'doc_id of the M-12 relatório, used for YoY context (NULL if fund <12mo old)';
COMMENT ON COLUMN gestores.cost_usd IS
    'API cost in USD to produce this row (extractor + judge combined)';
COMMENT ON COLUMN gestores.prompt_version IS
    'Hash or version tag of the prompt file used; lets us re-run all extractions when prompts change';
