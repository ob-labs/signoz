package ingest

import (
	"context"
	"time"
)

// Binary retention is independent of exact source retention: originals remain
// reconstructable for the lifetime of their Trace. Orphans are bounded to 30d.
func (ep *endpoint) maintainTraceMedia() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := ep.db.ExecContext(ctx, "DELETE FROM "+tableName(ep.cfg.TablePrefix, "trace_media")+" WHERE created_at < ? LIMIT 256", time.Now().Add(-30*24*time.Hour))
		if err != nil {
			ep.logger.Warn("trace media cleanup failed", "error", err)
		}
		_, err = ep.db.ExecContext(ctx, "DELETE FROM "+tableName(ep.cfg.TablePrefix, "trace_media_original")+" WHERE created_at < ? AND NOT EXISTS (SELECT 1 FROM "+ep.tracesTable+" t WHERE t.org_id="+tableName(ep.cfg.TablePrefix, "trace_media_original")+".org_id AND t.space_id="+tableName(ep.cfg.TablePrefix, "trace_media_original")+".space_id AND t.trace_id="+tableName(ep.cfg.TablePrefix, "trace_media_original")+".trace_id AND t.span_id="+tableName(ep.cfg.TablePrefix, "trace_media_original")+".span_id) LIMIT 256", time.Now().Add(-30*24*time.Hour))
		if err != nil {
			ep.logger.Warn("trace media original cleanup failed", "error", err)
		}
		cancel()
		<-ticker.C
	}
}
