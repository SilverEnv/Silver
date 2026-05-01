ALTER TABLE silver.analytics_runs
    DROP CONSTRAINT analytics_runs_run_kind_check;

ALTER TABLE silver.analytics_runs
    ADD CONSTRAINT analytics_runs_run_kind_check
    CHECK (
        run_kind IN (
            'price_normalization',
            'label_generation',
            'feature_generation',
            'backtest',
            'falsifier_report_invocation',
            'sec_companyfacts_ingest',
            'fmp_fundamentals_normalization'
        )
    );

CREATE TABLE silver.fundamental_values (
    id bigserial PRIMARY KEY,
    security_id bigint NOT NULL REFERENCES silver.securities(id),
    period_end_date date NOT NULL,
    fiscal_year integer NOT NULL,
    fiscal_period text NOT NULL,
    period_type text NOT NULL,
    statement_type text NOT NULL,
    metric_name text NOT NULL,
    metric_value numeric(28, 6) NOT NULL,
    currency text NOT NULL,
    source_system text NOT NULL,
    source_field text NOT NULL,
    raw_object_id bigint NOT NULL REFERENCES silver.raw_objects(id),
    accepted_at timestamptz NOT NULL,
    filing_date date NOT NULL,
    available_at timestamptz NOT NULL,
    available_at_policy_id bigint NOT NULL REFERENCES silver.available_at_policies(id),
    normalized_by_run_id bigint NOT NULL REFERENCES silver.analytics_runs(id),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (
        security_id,
        period_end_date,
        period_type,
        statement_type,
        metric_name,
        source_system
    ),
    CHECK (fiscal_year BETWEEN 1900 AND 2100),
    CHECK (fiscal_period IN ('FY', 'Q1', 'Q2', 'Q3', 'Q4')),
    CHECK (period_type IN ('annual', 'quarterly')),
    CHECK (statement_type IN ('income_statement', 'cash_flow_statement')),
    CHECK (btrim(metric_name) <> ''),
    CHECK (btrim(currency) <> ''),
    CHECK (btrim(source_system) <> ''),
    CHECK (btrim(source_field) <> ''),
    CHECK (available_at > accepted_at),
    CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX fundamental_values_metric_period_idx
    ON silver.fundamental_values (metric_name, period_type, period_end_date);

CREATE INDEX fundamental_values_security_period_idx
    ON silver.fundamental_values (security_id, period_type, period_end_date);

CREATE INDEX fundamental_values_raw_object_idx
    ON silver.fundamental_values (raw_object_id);
