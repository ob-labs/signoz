-- Legacy manual repair artifact; ordinary functional rollback uses the two emergency controls.
-- Suspend projection maintenance fleet-wide before schema maintenance.
-- Standard startup migration repairs missing columns; do not use this as a query toggle.
-- Raw spans and canonical row_json are retained.
ALTER TABLE `signoz_trace_projections`
 DROP COLUMN session_trace_tokens_v1,
 DROP COLUMN session_billable_v1;
