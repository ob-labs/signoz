-- First disable SIGNOZ_AIVISION_SESSION_TOKEN_GENERATED_COLUMNS_ENABLED
-- and restart/verify query workers. Keep row_json and all raw evidence.
-- Use a separate maintenance window to reverse the optional storage change.
ALTER TABLE `signoz_trace_projections`
 DROP COLUMN session_trace_tokens_v1,
 DROP COLUMN session_billable_v1;
