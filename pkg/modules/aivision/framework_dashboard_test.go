package aivision

import (
	"strings"
	"testing"
)

func TestFrameworkDashboardEventStreamStatement(t *testing.T) {
	store := &oceanBaseStore{cfg: config{TablePrefix: "signoz", MaxLimit: 500}}
	request := dashboardRequest{
		QueryKey:     "framework.overview.event_stream",
		From:         1_700_000_000_000,
		To:           1_700_003_600_000,
		SpaceID:      "space-7",
		AgentProduct: "langfuse",
		Params:       map[string]any{"limit": 20},
	}
	where, args, err := dashboardWhere("org-3", request)
	if err != nil {
		t.Fatal(err)
	}

	statement, handled, err := buildFrameworkDashboardStatement(store, request, where, args)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("framework event_stream was not handled")
	}
	sql := statement.SQL
	for _, want := range []string{
		"`signoz_traces`",
		"ORDER BY start_time_unix_nano DESC LIMIT ?",
		") IN ('llm', 'tool')",
		"AS kind",
		"duration_nano / 1000000 AS cost_ms",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("event_stream SQL missing %q\n%s", want, sql)
		}
	}
	// limit is the final arg and is passed through; tenant scope is preserved.
	if got := statement.Args[len(statement.Args)-1]; got != 20 {
		t.Fatalf("expected limit 20 as last arg, got %v", got)
	}
	if !strings.Contains(sql, "org_id = ?") || !strings.Contains(sql, "space_id = ?") || !strings.Contains(sql, "agent_product = ?") {
		t.Fatalf("event_stream SQL lost tenant scope\n%s", sql)
	}
}

func TestFrameworkDashboardEventStreamAgentInstanceFilter(t *testing.T) {
	store := &oceanBaseStore{cfg: config{TablePrefix: "signoz", MaxLimit: 500}}
	request := dashboardRequest{
		QueryKey: "framework.overview.event_stream",
		From:     1_700_000_000_000,
		To:       1_700_003_600_000,
		SpaceID:  "space-7",
		Params:   map[string]any{"agent_uuid": "agent-abc"},
	}
	where, args, err := dashboardWhere("org-3", request)
	if err != nil {
		t.Fatal(err)
	}
	statement, handled, err := buildFrameworkDashboardStatement(store, request, where, args)
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if !strings.Contains(statement.SQL, `JSON_UNQUOTE(JSON_EXTRACT(resource_attributes, '$."aivision.agent.id"')) = ?`) {
		t.Fatalf("agent_uuid filter missing\n%s", statement.SQL)
	}
	found := false
	for _, arg := range statement.Args {
		if arg == "agent-abc" {
			found = true
		}
	}
	if !found {
		t.Fatalf("agent_uuid arg not bound: %v", statement.Args)
	}
}

func TestFrameworkDashboardEventsTimeseries(t *testing.T) {
	store := &oceanBaseStore{cfg: config{TablePrefix: "signoz", MaxLimit: 500}}
	request := dashboardRequest{
		QueryKey: "framework.events.timeseries",
		From:     1_700_000_000_000,
		To:       1_700_003_600_000,
		SpaceID:  "space-7",
		Params:   map[string]any{"step_ms": 60_000},
	}
	where, args, err := dashboardWhere("org-3", request)
	if err != nil {
		t.Fatal(err)
	}
	statement, handled, err := buildFrameworkDashboardStatement(store, request, where, args)
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if !strings.Contains(statement.SQL, "GROUP BY time, kind ORDER BY time ASC") {
		t.Fatalf("timeseries grouping missing\n%s", statement.SQL)
	}
	// step_ms is bound twice (DIV ? and * ?) ahead of the scope args.
	if statement.Args[0] != int64(60_000) || statement.Args[1] != int64(60_000) {
		t.Fatalf("expected step bound twice first, got %v", statement.Args[:2])
	}
}

func TestFrameworkDashboardRejectsForeignKeys(t *testing.T) {
	store := &oceanBaseStore{cfg: config{TablePrefix: "signoz", MaxLimit: 500}}
	for _, queryKey := range []string{
		"common.overview.event_stream",
		"framework.unknown_panel",
	} {
		request := dashboardRequest{QueryKey: queryKey, SpaceID: "space-7", From: 1, To: 2}
		_, handled, err := buildFrameworkDashboardStatement(store, request, "org_id = ?", []any{"org-3"})
		if err != nil {
			t.Fatalf("%s: %v", queryKey, err)
		}
		if handled {
			t.Fatalf("%s should not be handled by the framework builder", queryKey)
		}
	}
}
