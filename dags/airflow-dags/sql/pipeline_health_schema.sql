-- One row per run of the aqi_pipeline_health DAG.
CREATE TABLE IF NOT EXISTS pipeline_health_report (
    id             BIGSERIAL   PRIMARY KEY,
    checked_at     TIMESTAMPTZ NOT NULL,
    overall_status TEXT        NOT NULL,   -- healthy | degraded | failing
    source         TEXT        NOT NULL,   -- ai: Claude's verdict; rules: rule checks only
    summary        TEXT        NOT NULL,
    issues         JSONB       NOT NULL,   -- [{component, severity, finding, evidence, suggested_action}]
    rule_findings  JSONB       NOT NULL,   -- what the deterministic checks found
    signals        JSONB       NOT NULL,   -- everything that was collected
    ai_status      TEXT        NOT NULL,   -- ok | skipped | refused | error
    ai_model       TEXT,
    input_tokens   INTEGER,
    output_tokens  INTEGER,
    report_key     TEXT                    -- Markdown copy in the MinIO bucket aqi-images
);

CREATE INDEX IF NOT EXISTS pipeline_health_report_checked_at_index
    ON pipeline_health_report USING btree (checked_at);
