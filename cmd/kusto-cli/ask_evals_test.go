package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type askEvalCase struct {
	Name                 string             `json:"name"`
	Description          string             `json:"description"`
	Args                 []string           `json:"args"`
	Config               askEvalConfig      `json:"config"`
	AgentQuery           string             `json:"agent_query,omitempty"`
	SchemaWithSampleRows bool               `json:"schema_with_sample_rows,omitempty"`
	ShotsRows            []askEvalShotRow   `json:"shots_rows,omitempty"`
	Want                 askEvalExpectation `json:"want"`
}

type askEvalConfig struct {
	ServiceURI    string               `json:"service_uri,omitempty"`
	Database      string               `json:"database,omitempty"`
	TargetAlias   string               `json:"target_alias,omitempty"`
	KnownServices []KustoServiceConfig `json:"known_services,omitempty"`
}

type askEvalShotRow struct {
	Name   string `json:"name"`
	Intent string `json:"intent"`
	Query  string `json:"query"`
}

type askEvalExpectation struct {
	Success                 bool             `json:"success"`
	AgentCalled             *bool            `json:"agent_called,omitempty"`
	ErrorContains           []string         `json:"error_contains,omitempty"`
	Target                  queryDraftTarget `json:"target,omitempty"`
	Query                   string           `json:"query,omitempty"`
	ValidationStatus        string           `json:"validation_status,omitempty"`
	SafeForExecution        *bool            `json:"safe_for_execution,omitempty"`
	ReadOnly                *bool            `json:"read_only,omitempty"`
	Bounded                 *bool            `json:"bounded,omitempty"`
	ExecutionExecuted       *bool            `json:"execution_executed,omitempty"`
	ExecutionStatus         string           `json:"execution_status,omitempty"`
	ExecutionReasonContains string           `json:"execution_reason_contains,omitempty"`
	ExecutionMaxRecords     int              `json:"execution_max_records,omitempty"`
	DisclosureMode          string           `json:"disclosure_mode,omitempty"`
	Sent                    askEvalSent      `json:"sent,omitempty"`
	ExamplesInclude         []string         `json:"examples_include,omitempty"`
	WarningsContain         []string         `json:"warnings_contain,omitempty"`
	ValidationErrorsContain []string         `json:"validation_errors_contain,omitempty"`
	OutputContains          []string         `json:"output_contains,omitempty"`
	OutputNotContains       []string         `json:"output_not_contains,omitempty"`
	RequestSampleRows       *bool            `json:"request_sample_rows,omitempty"`
	KustoCalls              *int             `json:"kusto_calls,omitempty"`
	ReadonlyKustoCalls      *int             `json:"readonly_kusto_calls,omitempty"`
	RequestMaxRecords       int              `json:"request_max_records,omitempty"`
}

type askEvalSent struct {
	Schema       *bool `json:"schema,omitempty"`
	Docstrings   *bool `json:"docstrings,omitempty"`
	Shots        *bool `json:"shots,omitempty"`
	SampleRows   *bool `json:"sample_rows,omitempty"`
	QueryResults *bool `json:"query_results,omitempty"`
}

type askEvalAgent struct {
	query  string
	called bool
	req    queryDraftRequest
}

func (a *askEvalAgent) GenerateQueryDraft(ctx context.Context, req queryDraftRequest) (queryDraft, error) {
	a.called = true
	a.req = req
	if strings.TrimSpace(a.query) == "" {
		return fakeModelProvider{}.GenerateQueryDraft(ctx, req)
	}
	return queryDraft{
		Format:         "query_draft",
		Target:         req.Target,
		Prompt:         req.Prompt,
		Query:          a.query,
		Assumptions:    []string{"Offline eval fake provider response."},
		Warnings:       []string{"Review the Query Draft before using an execution gate."},
		Examples:       req.Examples,
		SchemaContext:  req.SchemaContext,
		DataDisclosure: req.DataDisclosure,
	}, nil
}

type askEvalKustoCall struct {
	kind       string
	query      string
	readonly   bool
	maxRecords int
}

func TestAskQueryDraftOfflineEvals(t *testing.T) {
	cases := loadAskEvalCases(t)
	if len(cases) == 0 {
		t.Fatal("no ask eval cases loaded")
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			runAskEvalCase(t, tc)
		})
	}
}

func loadAskEvalCases(t *testing.T) []askEvalCase {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to locate ask eval test file")
	}
	path := filepath.Join(filepath.Dir(file), "..", "..", "evals", "ask", "query_draft.jsonl")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open ask eval fixtures: %v", err)
	}
	defer f.Close()

	var cases []askEvalCase
	scanner := bufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var tc askEvalCase
		if err := json.Unmarshal([]byte(line), &tc); err != nil {
			t.Fatalf("parse %s:%d: %v", path, lineNo, err)
		}
		if tc.Name == "" {
			t.Fatalf("parse %s:%d: missing case name", path, lineNo)
		}
		if len(tc.Args) == 0 {
			t.Fatalf("parse %s:%d: missing args", path, lineNo)
		}
		cases = append(cases, tc)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan ask eval fixtures: %v", err)
	}
	return cases
}

func runAskEvalCase(t *testing.T, tc askEvalCase) {
	t.Helper()
	var out bytes.Buffer
	agent := &askEvalAgent{query: tc.AgentQuery}
	schema := fakeStormSchemaContext()
	if tc.SchemaWithSampleRows {
		schema.Tables[0].SampleRows = []map[string]any{{
			"StartTime": "2026-01-01T00:00:00Z",
			"State":     "SampleValueShouldNotAppear",
			"EventType": "Hail",
		}}
	}
	calls := []askEvalKustoCall{}
	s := &server{
		cfg:              askEvalServerConfig(t, tc.Config),
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: schema},
		executeHook: func(_ context.Context, _, _, csl, kind string, readonly bool, crp map[string]any) (kustoResponse, error) {
			call := askEvalKustoCall{kind: kind, query: csl, readonly: readonly}
			if crp != nil {
				if v, ok := crp["query_take_max_records"].(int); ok {
					call.maxRecords = v
				}
			}
			calls = append(calls, call)
			if strings.Contains(csl, "QueryDraftExamples") {
				return askEvalShotsResponse(tc.ShotsRows), nil
			}
			if kind == "mgmt" {
				return makeKustoResponse([]kustoColumn{{ColumnName: "Plan", ColumnType: "string"}}, [][]any{{"validated"}}), nil
			}
			return makeKustoResponse([]kustoColumn{{ColumnName: "EventType", ColumnType: "string"}}, [][]any{{"Hail"}}), nil
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}

	err := s.runCommand(context.Background(), tc.Args)
	if !tc.Want.Success {
		if err == nil {
			t.Fatalf("expected eval failure, got success output:\n%s", out.String())
		}
		for _, want := range tc.Want.ErrorContains {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not contain %q", err.Error(), want)
			}
		}
		assertAskEvalBool(t, "agent called", agent.called, tc.Want.AgentCalled)
		assertAskEvalKustoCalls(t, calls, tc.Want)
		if out.Len() != 0 {
			t.Fatalf("failed eval wrote output: %q", out.String())
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected eval failure: %v", err)
	}
	assertAskEvalBool(t, "agent called", agent.called, tc.Want.AgentCalled)

	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
	}
	assertAskEvalDraft(t, tc, draft, agent, calls, out.String())
}

func askEvalServerConfig(t *testing.T, evalCfg askEvalConfig) config {
	t.Helper()
	cfg := config{
		serviceURI:  evalCfg.ServiceURI,
		database:    evalCfg.Database,
		targetAlias: evalCfg.TargetAlias,
		output:      "json",
	}
	if len(evalCfg.KnownServices) > 0 {
		b, err := json.Marshal(evalCfg.KnownServices)
		if err != nil {
			t.Fatalf("marshal known_services: %v", err)
		}
		cfg.knownServices = string(b)
	}
	return cfg
}

func askEvalShotsResponse(rows []askEvalShotRow) kustoResponse {
	out := make([][]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, []any{row.Name, row.Intent, row.Query})
	}
	return makeKustoResponse([]kustoColumn{
		{ColumnName: "name", ColumnType: "string"},
		{ColumnName: "intent", ColumnType: "string"},
		{ColumnName: "query", ColumnType: "string"},
	}, out)
}

func assertAskEvalDraft(t *testing.T, tc askEvalCase, draft queryDraft, agent *askEvalAgent, calls []askEvalKustoCall, output string) {
	t.Helper()
	want := tc.Want
	if draft.Format != "query_draft" {
		t.Fatalf("format = %q; want query_draft", draft.Format)
	}
	if want.Target.ClusterURI != "" || want.Target.Database != "" {
		if draft.Target != want.Target {
			t.Fatalf("target = %#v; want %#v", draft.Target, want.Target)
		}
		if agent.called && agent.req.Target != want.Target {
			t.Fatalf("provider request target = %#v; want %#v", agent.req.Target, want.Target)
		}
	}
	if want.Query != "" && draft.Query != want.Query {
		t.Fatalf("query = %q; want %q", draft.Query, want.Query)
	}
	if want.ValidationStatus != "" && draft.Validation.Status != want.ValidationStatus {
		t.Fatalf("validation status = %q; want %q (validation=%#v)", draft.Validation.Status, want.ValidationStatus, draft.Validation)
	}
	assertAskEvalBool(t, "validation.safe_for_execution", draft.Validation.SafeForExecution, want.SafeForExecution)
	assertAskEvalBool(t, "validation.read_only", draft.Validation.ReadOnly, want.ReadOnly)
	assertAskEvalBool(t, "validation.bounded", draft.Validation.Bounded, want.Bounded)
	assertAskEvalBool(t, "execution.executed", draft.Execution.Executed, want.ExecutionExecuted)
	if want.ExecutionStatus != "" && draft.Execution.Status != want.ExecutionStatus {
		t.Fatalf("execution status = %q; want %q (execution=%#v)", draft.Execution.Status, want.ExecutionStatus, draft.Execution)
	}
	if want.ExecutionReasonContains != "" && !strings.Contains(strings.ToLower(draft.Execution.Reason), strings.ToLower(want.ExecutionReasonContains)) {
		t.Fatalf("execution reason = %q; want to contain %q", draft.Execution.Reason, want.ExecutionReasonContains)
	}
	if want.ExecutionMaxRecords != 0 && draft.Execution.MaxRecords != want.ExecutionMaxRecords {
		t.Fatalf("execution max_records = %d; want %d", draft.Execution.MaxRecords, want.ExecutionMaxRecords)
	}
	if want.DisclosureMode != "" && draft.DataDisclosure.Mode != want.DisclosureMode {
		t.Fatalf("disclosure mode = %q; want %q", draft.DataDisclosure.Mode, want.DisclosureMode)
	}
	assertAskEvalDisclosure(t, draft.DataDisclosure.Sent, want.Sent)
	if agent.called {
		assertAskEvalDisclosure(t, agent.req.DataDisclosure.Sent, want.Sent)
	}
	for _, name := range want.ExamplesInclude {
		if !askEvalExamplesContain(draft.Examples, name) {
			t.Fatalf("examples did not include %q: %#v", name, draft.Examples)
		}
	}
	for _, wantText := range want.WarningsContain {
		if !strings.Contains(strings.Join(draft.Warnings, "\n"), wantText) {
			t.Fatalf("warnings %#v do not contain %q", draft.Warnings, wantText)
		}
	}
	for _, wantText := range want.ValidationErrorsContain {
		if !strings.Contains(strings.Join(draft.Validation.Errors, "\n"), wantText) {
			t.Fatalf("validation errors %#v do not contain %q", draft.Validation.Errors, wantText)
		}
	}
	for _, wantText := range want.OutputContains {
		if !strings.Contains(output, wantText) {
			t.Fatalf("output did not contain %q\n%s", wantText, output)
		}
	}
	for _, forbidden := range want.OutputNotContains {
		if strings.Contains(output, forbidden) {
			t.Fatalf("output contained forbidden %q\n%s", forbidden, output)
		}
	}
	if want.RequestSampleRows != nil {
		got := false
		if agent.called && len(agent.req.SchemaContext) > 0 && len(agent.req.SchemaContext[0].Tables) > 0 {
			got = len(agent.req.SchemaContext[0].Tables[0].SampleRows) > 0
		}
		assertAskEvalBool(t, "provider request sample rows", got, want.RequestSampleRows)
	}
	assertAskEvalKustoCalls(t, calls, want)
}

func assertAskEvalDisclosure(t *testing.T, got queryDraftDataDisclosureSent, want askEvalSent) {
	t.Helper()
	assertAskEvalBool(t, "disclosure.schema", got.Schema, want.Schema)
	assertAskEvalBool(t, "disclosure.docstrings", got.Docstrings, want.Docstrings)
	assertAskEvalBool(t, "disclosure.shots", got.Shots, want.Shots)
	assertAskEvalBool(t, "disclosure.sample_rows", got.SampleRows, want.SampleRows)
	assertAskEvalBool(t, "disclosure.query_results", got.QueryResults, want.QueryResults)
}

func assertAskEvalKustoCalls(t *testing.T, calls []askEvalKustoCall, want askEvalExpectation) {
	t.Helper()
	if want.KustoCalls != nil && len(calls) != *want.KustoCalls {
		t.Fatalf("Kusto calls = %d (%s); want %d", len(calls), formatAskEvalKustoCalls(calls), *want.KustoCalls)
	}
	if want.ReadonlyKustoCalls != nil {
		readonly := 0
		for _, call := range calls {
			if call.readonly {
				readonly++
			}
		}
		if readonly != *want.ReadonlyKustoCalls {
			t.Fatalf("readonly Kusto calls = %d (%s); want %d", readonly, formatAskEvalKustoCalls(calls), *want.ReadonlyKustoCalls)
		}
	}
	if want.RequestMaxRecords != 0 {
		for _, call := range calls {
			if call.maxRecords == want.RequestMaxRecords {
				return
			}
		}
		t.Fatalf("no Kusto call used query_take_max_records=%d; calls=%s", want.RequestMaxRecords, formatAskEvalKustoCalls(calls))
	}
}

func assertAskEvalBool(t *testing.T, name string, got bool, want *bool) {
	t.Helper()
	if want == nil {
		return
	}
	if got != *want {
		t.Fatalf("%s = %t; want %t", name, got, *want)
	}
}

func askEvalExamplesContain(examples []queryDraftExample, name string) bool {
	for _, example := range examples {
		if example.Name == name {
			return true
		}
	}
	return false
}

func formatAskEvalKustoCalls(calls []askEvalKustoCall) string {
	parts := make([]string, 0, len(calls))
	for _, call := range calls {
		parts = append(parts, fmt.Sprintf("%s readonly=%t max=%d query=%q", call.kind, call.readonly, call.maxRecords, call.query))
	}
	return strings.Join(parts, "; ")
}
