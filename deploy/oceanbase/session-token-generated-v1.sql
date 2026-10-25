-- Legacy manual repair artifact for the default signoz table prefix.
-- Standard startup migration owns these columns; do not apply this to a healthy current schema.
-- Suspend projection maintenance before separately authorized manual schema repair.
ALTER TABLE `signoz_trace_projections`
 ADD COLUMN session_trace_tokens_v1 DECIMAL(65,0) GENERATED ALWAYS AS (CASE WHEN JSON_EXTRACT(row_json,'$.coverage.billableCalls')=0 THEN 0 WHEN JSON_EXTRACT(row_json,'$.coverage.billableCalls')>0 AND JSON_TYPE(JSON_EXTRACT(row_json,'$.usage.total')) IN ('INTEGER','UNSIGNED INTEGER','DOUBLE','DECIMAL') THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(row_json,'$.usage.total')) AS DECIMAL(65,0)) ELSE NULL END) STORED,
 ADD COLUMN session_billable_v1 TINYINT GENERATED ALWAYS AS (CASE WHEN JSON_EXTRACT(row_json,'$.coverage.billableCalls')>0 THEN 1 ELSE 0 END) STORED;
