package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	_ = os.Unsetenv("KUSTO_SHOTS_TABLE")
	os.Exit(m.Run())
}

type recordingQueryDraftAgent struct {
	called bool
	req    queryDraftRequest
}

func (a *recordingQueryDraftAgent) GenerateQueryDraft(_ context.Context, req queryDraftRequest) (queryDraft, error) {
	a.called = true
	a.req = req
	return queryDraft{
		Query:          "StormEvents | take 5",
		Assumptions:    []string{"Using the public sample StormEvents table."},
		Warnings:       []string{"Fake agent response for deterministic tests."},
		SchemaContext:  req.SchemaContext,
		DataDisclosure: req.DataDisclosure,
	}, nil
}

type scriptedQueryDraftAgent struct {
	called bool
	draft  queryDraft
}

func (a *scriptedQueryDraftAgent) GenerateQueryDraft(_ context.Context, req queryDraftRequest) (queryDraft, error) {
	a.called = true
	draft := a.draft
	if draft.SchemaContext == nil {
		draft.SchemaContext = req.SchemaContext
	}
	if draft.DataDisclosure.Mode == "" {
		draft.DataDisclosure = req.DataDisclosure
	}
	return draft, nil
}

type repairingQueryDraftAgent struct {
	generateDraft queryDraft
	repairDrafts  []queryDraft
	generateCalls int
	repairCalls   []queryDraftRepairRequest
}

func (a *repairingQueryDraftAgent) GenerateQueryDraft(_ context.Context, req queryDraftRequest) (queryDraft, error) {
	a.generateCalls++
	draft := a.generateDraft
	if draft.SchemaContext == nil {
		draft.SchemaContext = req.SchemaContext
	}
	if draft.DataDisclosure.Mode == "" {
		draft.DataDisclosure = req.DataDisclosure
	}
	return draft, nil
}

func (a *repairingQueryDraftAgent) RepairQueryDraft(_ context.Context, req queryDraftRepairRequest) (queryDraft, error) {
	a.repairCalls = append(a.repairCalls, req)
	idx := len(a.repairCalls) - 1
	if idx >= len(a.repairDrafts) {
		return queryDraft{}, errors.New("unexpected Repair Pass")
	}
	draft := a.repairDrafts[idx]
	if draft.SchemaContext == nil {
		draft.SchemaContext = req.SchemaContext
	}
	if draft.DataDisclosure.Mode == "" {
		draft.DataDisclosure = req.DataDisclosure
	}
	return draft, nil
}

type fakeSchemaDiscoverer struct {
	called bool
	req    schemaDiscoveryRequest
	ctx    queryDraftSchemaContext
	err    error
}

func (d *fakeSchemaDiscoverer) DiscoverSchemaContext(_ context.Context, req schemaDiscoveryRequest) (queryDraftSchemaContext, error) {
	d.called = true
	d.req = req
	return d.ctx, d.err
}

func runAskWithDraft(t *testing.T, draft queryDraft) queryDraft {
	t.Helper()
	var out bytes.Buffer
	agent := &scriptedQueryDraftAgent{draft: draft}
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		stdout:           &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if !agent.called {
		t.Fatal("Query Draft Agent was not called")
	}
	var got queryDraft
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
	}
	return got
}

func fakeStormSchemaContext() queryDraftSchemaContext {
	return queryDraftSchemaContext{
		Source:   "fake-schema",
		Entities: []string{"StormEvents", "RecentStorms"},
		Tables: []queryDraftSchemaTable{
			{
				Name:      "StormEvents",
				DocString: "Public sample storm event records.",
				Columns: []queryDraftSchemaColumn{
					{Name: "StartTime", Type: "datetime"},
					{Name: "State", Type: "string"},
					{Name: "EventType", Type: "string"},
				},
				SampleRows: []map[string]any{{"State": "WA", "EventType": "Hail"}},
			},
		},
		Functions: []queryDraftSchemaFunction{
			{
				Name:        "RecentStorms",
				DocString:   "Returns recent public sample storm events.",
				InputSchema: "()",
				OutputColumns: []queryDraftSchemaColumn{
					{Name: "StartTime", Type: "datetime"},
					{Name: "State", Type: "string"},
				},
			},
		},
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}

func TestValidateQueryAndCommand(t *testing.T) {
	if err := validateQuery("// comment\nStormEvents | count"); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}
	if err := validateQuery(".show version"); err == nil {
		t.Fatal("management command accepted as query")
	}
	if err := validateCommand(".show version"); err != nil {
		t.Fatalf("valid command rejected: %v", err)
	}
	if err := validateCommand("StormEvents | count"); err == nil {
		t.Fatal("query accepted as command")
	}
}

func TestCanonicalEntityType(t *testing.T) {
	cases := map[string]string{
		"tables":         "table",
		"mv":             "materialized-view",
		"external table": "external-table",
		"graphs":         "graph",
		"databases":      "database",
	}
	for in, want := range cases {
		got, err := canonicalEntityType(in)
		if err != nil || got != want {
			t.Fatalf("canonicalEntityType(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
}

func TestBuildRequestPropertiesBlocksReadonlyOverride(t *testing.T) {
	if _, err := buildRequestProperties(true, map[string]any{"request_readonly": false}); err == nil {
		t.Fatal("expected request_readonly override to be blocked")
	}
	props, err := buildRequestProperties(true, map[string]any{"servertimeout": "00:01:00"})
	if err != nil {
		t.Fatal(err)
	}
	opts := props["Options"].(map[string]any)
	if opts["request_readonly"] != true || opts["request_readonly_hardline"] != true {
		t.Fatalf("readonly flags not set: %#v", opts)
	}
}

func TestParseKustoV2Response(t *testing.T) {
	body := `[{
		"FrameType":"DataTable",
		"TableKind":"PrimaryResult",
		"Columns":[{"ColumnName":"Count","ColumnType":"long"}],
		"Rows":[[59066]]
	}]`
	got, err := parseKustoResponse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if got.Format != "kusto_response" || got.Data.Columns[0].ColumnName != "Count" || got.Data.Rows[0][0] != json.Number("59066") {
		t.Fatalf("unexpected parsed response: %#v", got)
	}
}

func TestParseKustoV1ResponseMapsDataType(t *testing.T) {
	body := `{"Tables":[{"TableName":"Table_0","Columns":[{"ColumnName":"BuildVersion","DataType":"String","ColumnType":"string"}],"Rows":[["1.0"]]}]}`
	got, err := parseKustoResponse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if got.Data.Columns[0].ColumnType != "string" || got.Data.Rows[0][0] != "1.0" {
		t.Fatalf("unexpected parsed response: %#v", got)
	}
}

func TestDeeplinkLooksRight(t *testing.T) {
	link := buildDeeplink("https://help.kusto.windows.net", "Samples", "StormEvents | count")
	if !strings.HasPrefix(link, "https://dataexplorer.azure.com/clusters/help.kusto.windows.net/databases/Samples?query=") {
		t.Fatalf("unexpected deeplink: %s", link)
	}
}

func TestAskMissingPromptReturnsUsageError(t *testing.T) {
	var out bytes.Buffer
	s := &server{
		cfg:    config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	err := s.runCommand(context.Background(), []string{"ask"})
	if err == nil || !strings.Contains(err.Error(), "usage: kusto-cli ask '<natural-language prompt>'") {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("missing prompt wrote output: %q", out.String())
	}
}

func TestAskUsesFakedQueryDraftAgentAndWritesStableJSON(t *testing.T) {
	var out bytes.Buffer
	agent := &recordingQueryDraftAgent{}
	schemaDiscoverer := &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()}
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: schemaDiscoverer,
		stdout:           &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "show", "recent", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if !agent.called {
		t.Fatal("fake Query Draft Agent was not called")
	}
	if !schemaDiscoverer.called {
		t.Fatal("schema discoverer was not called")
	}
	if agent.req.Prompt != "show recent storm events" {
		t.Fatalf("prompt = %q; want joined natural-language prompt", agent.req.Prompt)
	}
	if agent.req.Target.ClusterURI != "https://help.kusto.windows.net" || agent.req.Target.Database != "Samples" {
		t.Fatalf("target = %#v; want public sample target", agent.req.Target)
	}
	if schemaDiscoverer.req.IncludeSampleRows {
		t.Fatal("sample rows were requested without explicit opt-in")
	}
	if len(agent.req.Examples) == 0 || agent.req.Examples[0].Source != "bundled" {
		t.Fatalf("examples sent to Query Draft Agent = %#v; want bundled examples", agent.req.Examples)
	}
	if !agent.req.DataDisclosure.Sent.Shots {
		t.Fatal("Data Disclosure Policy did not report bundled examples as shots sent to the model provider")
	}

	want := `{
  "format": "query_draft",
  "target": {
    "cluster_uri": "https://help.kusto.windows.net",
    "database": "Samples"
  },
  "prompt": "show recent storm events",
  "query": "StormEvents | take 5",
  "assumptions": [
    "Using the public sample StormEvents table."
  ],
  "warnings": [
    "Fake agent response for deterministic tests."
  ],
  "examples": [
    {
      "source": "bundled",
      "name": "recent_rows",
      "intent": "Return recent rows with an explicit result bound.",
      "query": "StormEvents\n| where StartTime > ago(1d)\n| project StartTime, State, EventType\n| take 100"
    },
    {
      "source": "bundled",
      "name": "filter_project_take",
      "intent": "Filter rows, select relevant columns, and cap returned records.",
      "query": "StormEvents\n| where State == 'WA'\n| project StartTime, State, EventType\n| take 100"
    },
    {
      "source": "bundled",
      "name": "summarize_count_by_dimension",
      "intent": "Count rows by a categorical column and return the most common values.",
      "query": "StormEvents\n| summarize EventCount = count() by State\n| top 10 by EventCount desc"
    }
  ],
  "schema_context": [
    {
      "source": "fake-schema",
      "entities": [
        "StormEvents",
        "RecentStorms"
      ],
      "tables": [
        {
          "name": "StormEvents",
          "docstring": "Public sample storm event records.",
          "columns": [
            {
              "name": "StartTime",
              "type": "datetime"
            },
            {
              "name": "State",
              "type": "string"
            },
            {
              "name": "EventType",
              "type": "string"
            }
          ]
        }
      ],
      "functions": [
        {
          "name": "RecentStorms",
          "docstring": "Returns recent public sample storm events.",
          "input_schema": "()",
          "output_columns": [
            {
              "name": "StartTime",
              "type": "datetime"
            },
            {
              "name": "State",
              "type": "string"
            }
          ]
        }
      ]
    }
  ],
  "data_disclosure_policy": {
    "mode": "schema-only",
    "sent_to_model_provider": {
      "schema": true,
      "docstrings": true,
      "shots": true,
      "sample_rows": false,
      "query_results": false
    }
  },
  "validation": {
    "status": "passed",
    "read_only": true,
    "bounded": true,
    "safe_for_execution": true,
    "checks": [
      {
        "name": "query_not_empty",
        "passed": true
      },
      {
        "name": "raw_kql_only",
        "passed": true
      },
      {
        "name": "no_management_commands",
        "passed": true
      },
      {
        "name": "no_write_capable_or_destructive_output",
        "passed": true
      },
      {
        "name": "safe_statement_shape",
        "passed": true
      },
      {
        "name": "bounded_result",
        "passed": true
      }
    ],
    "warnings": [],
    "errors": []
  },
  "execution": {
    "executed": false,
    "reason": "generate-only; execution requires an explicit execution gate"
  }
}
`
	if out.String() != want {
		t.Fatalf("ask JSON output mismatch\ngot:\n%s\nwant:\n%s", out.String(), want)
	}

	var generic map[string]any
	if err := json.Unmarshal(out.Bytes(), &generic); err != nil {
		t.Fatal(err)
	}
	if _, ok := generic["execution_result"]; ok {
		t.Fatal("ask output must not include an execution result")
	}
	if strings.Contains(out.String(), "Hail") {
		t.Fatal("ask output included raw sample row values without explicit opt-in")
	}
}

func TestAskInvalidShotsLimitReturnsUsageError(t *testing.T) {
	var out bytes.Buffer
	s := &server{
		cfg:    config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	err := s.runCommand(context.Background(), []string{"ask", "--shots-limit=abc", "show", "storm", "events"})
	if err == nil || !strings.Contains(err.Error(), "--shots-limit requires an integer value") {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("invalid shots flag wrote output: %q", out.String())
	}
}

func TestAskRetrievesConfiguredShots(t *testing.T) {
	var out bytes.Buffer
	agent := &recordingQueryDraftAgent{}
	shotsQueries := []string{}
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(_ context.Context, clusterURI, database, csl, kind string, readonly bool, _ map[string]any) (kustoResponse, error) {
			if clusterURI != "https://help.kusto.windows.net" || database != "Samples" || kind != "query" || !readonly {
				t.Fatalf("shots query execution = cluster=%q database=%q kind=%q readonly=%t", clusterURI, database, kind, readonly)
			}
			shotsQueries = append(shotsQueries, csl)
			return makeKustoResponse(
				[]kustoColumn{{ColumnName: "Name", ColumnType: "string"}, {ColumnName: "Prompt", ColumnType: "string"}, {ColumnName: "Query", ColumnType: "string"}},
				[][]any{{"wa_storms", "show WA storms", "StormEvents\n| where State == 'WA'\n| take 10"}},
			), nil
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--shots-table", "QueryShots", "--shots-limit", "2", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if !agent.called {
		t.Fatal("Query Draft Agent was not called")
	}
	if len(shotsQueries) != 1 {
		t.Fatalf("shots queries = %#v; want exactly one configured shots query", shotsQueries)
	}
	if !strings.Contains(shotsQueries[0], "['QueryShots']") || !strings.Contains(shotsQueries[0], "'show storm events'") || !strings.Contains(shotsQueries[0], "take 2") {
		t.Fatalf("shots query = %q; want table, prompt, and configured limit", shotsQueries[0])
	}
	if len(agent.req.Examples) < 2 || agent.req.Examples[0].Source != "configured" || agent.req.Examples[0].Name != "wa_storms" {
		t.Fatalf("examples sent to Query Draft Agent = %#v; want configured shot before bundled examples", agent.req.Examples)
	}
	if agent.req.Examples[0].Query != "StormEvents\n| where State == 'WA'\n| take 10" {
		t.Fatalf("configured shot query = %q", agent.req.Examples[0].Query)
	}
	if !agent.req.DataDisclosure.Sent.Shots {
		t.Fatal("Data Disclosure Policy did not report configured shots sent to the model provider")
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
	}
	if len(draft.Examples) == 0 || draft.Examples[0].Source != "configured" {
		t.Fatalf("output examples = %#v; want configured shot reported", draft.Examples)
	}
}

func TestAskMissingConfiguredShotsStillUsesBundledExamples(t *testing.T) {
	var out bytes.Buffer
	agent := &recordingQueryDraftAgent{}
	executed := false
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(context.Context, string, string, string, string, bool, map[string]any) (kustoResponse, error) {
			executed = true
			return kustoResponse{}, nil
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if executed {
		t.Fatal("ask queried a configured shots source even though none was configured")
	}
	if len(agent.req.Examples) == 0 || agent.req.Examples[0].Source != "bundled" {
		t.Fatalf("examples = %#v; want bundled examples when configured shots are missing", agent.req.Examples)
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatal(err)
	}
	if !draft.DataDisclosure.Sent.Shots || len(draft.Examples) == 0 {
		t.Fatalf("disclosure/examples = %#v / %#v; want bundled examples reported as shots", draft.DataDisclosure, draft.Examples)
	}
	if containsString(draft.Warnings, "Configured shots") {
		t.Fatalf("warnings = %#v; missing shots configuration should not warn", draft.Warnings)
	}
}

func TestAskConfiguredShotsFailureWarnsAndContinues(t *testing.T) {
	var out bytes.Buffer
	agent := &recordingQueryDraftAgent{}
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(context.Context, string, string, string, string, bool, map[string]any) (kustoResponse, error) {
			return kustoResponse{}, errors.New("shots table unavailable")
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--shots-table", "QueryShots", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if !agent.called {
		t.Fatal("Query Draft Agent was not called after shots retrieval failed")
	}
	if len(agent.req.Examples) == 0 || agent.req.Examples[0].Source != "bundled" {
		t.Fatalf("examples = %#v; want bundled examples after configured shots failure", agent.req.Examples)
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatal(err)
	}
	if !containsString(draft.Warnings, "Configured shots retrieval failed") {
		t.Fatalf("warnings = %#v; want configured shots warning", draft.Warnings)
	}
}

func TestBundledQueryDraftExamplesArePublicReadOnlyKQL(t *testing.T) {
	examples := bundledQueryDraftExamples("count recent events by state")
	if len(examples) == 0 {
		t.Fatal("expected bundled Query Draft examples")
	}
	for _, example := range examples {
		if example.Source != "bundled" {
			t.Fatalf("example source = %q; want bundled", example.Source)
		}
		if err := validateQueryDraftExample(example.Query); err != nil {
			t.Fatalf("bundled example %q is not safe read-only KQL: %v", example.Name, err)
		}
		lower := strings.ToLower(example.Intent + " " + example.Query)
		for _, forbidden := range []string{"cluster", "database", "customer", "incident", "internal"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("bundled example %q contains forbidden public-doc term %q: %s", example.Name, forbidden, example.Query)
			}
		}
	}
}

func TestConfiguredShotsUnsafeQueriesAreNotSent(t *testing.T) {
	resp := makeKustoResponse(
		[]kustoColumn{{ColumnName: "Name", ColumnType: "string"}, {ColumnName: "Prompt", ColumnType: "string"}, {ColumnName: "Query", ColumnType: "string"}},
		[][]any{
			{"drop_table", "delete old data", ".drop table StormEvents"},
			{"safe_take", "show rows", "StormEvents | take 5"},
		},
	)
	examples, warnings := queryDraftExamplesFromShotsResponse(resp)
	if len(examples) != 1 || examples[0].Name != "safe_take" {
		t.Fatalf("examples = %#v; want only safe read-only shot", examples)
	}
	if !containsString(warnings, "not a safe read-only Query Draft example") {
		t.Fatalf("warnings = %#v; want unsafe shot warning", warnings)
	}
}

func TestAskGenerateOnlyDoesNotExecuteWithoutExecutionGate(t *testing.T) {
	var out bytes.Buffer
	agent := &recordingQueryDraftAgent{}
	executed := false
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(context.Context, string, string, string, string, bool, map[string]any) (kustoResponse, error) {
			executed = true
			return kustoResponse{}, nil
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "show", "recent", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if executed {
		t.Fatal("generate-only ask executed generated KQL without the Execution Gate")
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
	}
	if draft.Execution.Executed {
		t.Fatalf("execution = %#v; want generate-only not executed", draft.Execution)
	}
	if !strings.Contains(draft.Execution.Reason, "execution requires an explicit execution gate") {
		t.Fatalf("execution reason = %q; want explicit gate message", draft.Execution.Reason)
	}
	if draft.Execution.Result != nil {
		t.Fatalf("execution result = %#v; want none in generate-only mode", draft.Execution.Result)
	}
}

func TestAskExecuteRequiresPassedValidation(t *testing.T) {
	var out bytes.Buffer
	agent := &scriptedQueryDraftAgent{draft: queryDraft{Query: "StormEvents | where State == 'WA'"}}
	executed := false
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(context.Context, string, string, string, string, bool, map[string]any) (kustoResponse, error) {
			executed = true
			return kustoResponse{}, nil
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--execute", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if executed {
		t.Fatal("ask --execute executed a Query Draft that did not pass validation")
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
	}
	if draft.Validation.Status != "warning" || draft.Validation.SafeForExecution {
		t.Fatalf("validation = %#v; want warning and not safe for execution", draft.Validation)
	}
	if draft.Execution.Executed || draft.Execution.Status != "blocked" {
		t.Fatalf("execution = %#v; want blocked without execution", draft.Execution)
	}
	if draft.Execution.MaxRecords != defaultAskExecutionMaxRecords {
		t.Fatalf("execution max_records = %d; want default %d", draft.Execution.MaxRecords, defaultAskExecutionMaxRecords)
	}
	if !strings.Contains(draft.Execution.Reason, "Execution Gate requested") || !strings.Contains(draft.Execution.Reason, "result bound") {
		t.Fatalf("execution reason = %q; want validation block details", draft.Execution.Reason)
	}
}

func TestAskExecuteAppliesReadonlyAndRecordLimitsAndIncludesResult(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		wantMax int
	}{
		{name: "default max records", args: []string{"ask", "--execute", "show", "storm", "events"}, wantMax: defaultAskExecutionMaxRecords},
		{name: "custom max records", args: []string{"ask", "--execute", "--max-rows", "7", "show", "storm", "events"}, wantMax: 7},
		{name: "custom execute max records alias", args: []string{"ask", "--execute", "--execute-max-rows=8", "show", "storm", "events"}, wantMax: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			agent := &recordingQueryDraftAgent{}
			var called int
			var gotCluster, gotDatabase, gotQuery, gotKind string
			var gotReadonly bool
			var gotOptions map[string]any
			s := &server{
				cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
				queryDraftAgent:  agent,
				schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
				executeHook: func(_ context.Context, clusterURI, database, csl, kind string, readonly bool, crp map[string]any) (kustoResponse, error) {
					called++
					gotCluster, gotDatabase, gotQuery, gotKind = clusterURI, database, csl, kind
					gotReadonly = readonly
					props, err := buildRequestProperties(readonly, crp)
					if err != nil {
						return kustoResponse{}, err
					}
					gotOptions = props["Options"].(map[string]any)
					return makeKustoResponse(
						[]kustoColumn{{ColumnName: "EventType", ColumnType: "string"}},
						[][]any{{"Hail"}},
					), nil
				},
				stdout: &out,
			}
			if err := s.loadKnownServices(); err != nil {
				t.Fatal(err)
			}
			if err := s.runCommand(context.Background(), tc.args); err != nil {
				t.Fatal(err)
			}
			if called != 1 {
				t.Fatalf("execute calls = %d; want 1", called)
			}
			if gotCluster != "https://help.kusto.windows.net" || gotDatabase != "Samples" || gotQuery != "StormEvents | take 5" || gotKind != "query" {
				t.Fatalf("execution args = cluster=%q database=%q query=%q kind=%q", gotCluster, gotDatabase, gotQuery, gotKind)
			}
			if !gotReadonly {
				t.Fatal("ask --execute did not use the read-only execution path")
			}
			if gotOptions["request_readonly"] != true || gotOptions["request_readonly_hardline"] != true {
				t.Fatalf("request options = %#v; want read-only hardline properties", gotOptions)
			}
			if gotOptions["query_take_max_records"] != tc.wantMax {
				t.Fatalf("query_take_max_records = %#v; want %d", gotOptions["query_take_max_records"], tc.wantMax)
			}
			if agent.req.DataDisclosure.Sent.QueryResults {
				t.Fatal("query results were marked as sent to model provider before execution")
			}
			var draft queryDraft
			if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
				t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
			}
			if !draft.Execution.Executed || draft.Execution.Status != "succeeded" || draft.Execution.Result == nil {
				t.Fatalf("execution = %#v; want succeeded result", draft.Execution)
			}
			if draft.Execution.MaxRecords != tc.wantMax {
				t.Fatalf("execution max_records = %d; want %d", draft.Execution.MaxRecords, tc.wantMax)
			}
			if len(draft.Execution.Result.Data.Rows) != 1 || draft.Execution.Result.Data.Rows[0][0] != "Hail" {
				t.Fatalf("execution result = %#v; want returned Kusto row", draft.Execution.Result)
			}
			if draft.DataDisclosure.Sent.QueryResults {
				t.Fatal("Query Draft reported query results sent to the model provider")
			}
		})
	}
}

func TestAskExecuteValidatePlanRunsBeforeExecution(t *testing.T) {
	var out bytes.Buffer
	agent := &recordingQueryDraftAgent{}
	calls := []string{}
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(_ context.Context, clusterURI, database, csl, kind string, readonly bool, crp map[string]any) (kustoResponse, error) {
			calls = append(calls, kind+":"+csl)
			if clusterURI != "https://help.kusto.windows.net" || database != "Samples" || !readonly {
				return kustoResponse{}, errors.New("unexpected Kusto validation/execution target")
			}
			switch kind {
			case "mgmt":
				if csl != ".show queryplan <| StormEvents | take 5" {
					return kustoResponse{}, errors.New("unexpected query-plan validation command: " + csl)
				}
				return makeKustoResponse([]kustoColumn{{ColumnName: "Plan", ColumnType: "string"}}, [][]any{{"validated"}}), nil
			case "query":
				if csl != "StormEvents | take 5" {
					return kustoResponse{}, errors.New("unexpected execution query: " + csl)
				}
				return makeKustoResponse([]kustoColumn{{ColumnName: "EventType", ColumnType: "string"}}, [][]any{{"Hail"}}), nil
			default:
				return kustoResponse{}, errors.New("unexpected Kusto call kind: " + kind)
			}
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--execute", "--validate-plan", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || !strings.HasPrefix(calls[0], "mgmt:.show queryplan <|") || !strings.HasPrefix(calls[1], "query:StormEvents") {
		t.Fatalf("Kusto calls = %#v; want query-plan validation before execution", calls)
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
	}
	if draft.Validation.QueryPlan == nil || draft.Validation.QueryPlan.Status != "passed" {
		t.Fatalf("query-plan validation = %#v; want passed", draft.Validation.QueryPlan)
	}
	if !draft.Execution.Executed || draft.Execution.Status != "succeeded" {
		t.Fatalf("execution = %#v; want execution after plan validation", draft.Execution)
	}
	if draft.DataDisclosure.Sent.QueryResults || agent.req.DataDisclosure.Sent.QueryResults {
		t.Fatal("query results were marked as sent to model provider")
	}
}

func TestAskValidatePlanCanBeConfiguredWithEnvironment(t *testing.T) {
	t.Setenv("KUSTO_ASK_VALIDATE_PLAN", "true")
	var out bytes.Buffer
	planCalls := 0
	executionCalls := 0
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  &recordingQueryDraftAgent{},
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(_ context.Context, _, _, csl, kind string, _ bool, _ map[string]any) (kustoResponse, error) {
			switch kind {
			case "mgmt":
				planCalls++
				if !strings.HasPrefix(csl, ".show queryplan <|") {
					return kustoResponse{}, errors.New("unexpected validation command: " + csl)
				}
				return makeKustoResponse([]kustoColumn{{ColumnName: "Plan", ColumnType: "string"}}, [][]any{{"validated"}}), nil
			case "query":
				executionCalls++
				return makeKustoResponse([]kustoColumn{{ColumnName: "EventType", ColumnType: "string"}}, [][]any{{"Hail"}}), nil
			default:
				return kustoResponse{}, errors.New("unexpected Kusto call kind: " + kind)
			}
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--execute", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if planCalls != 1 || executionCalls != 1 {
		t.Fatalf("plan calls = %d execution calls = %d; want configured plan validation before execution", planCalls, executionCalls)
	}
}

func TestAskExecuteValidatePlanFailureDoesNotExecuteWithoutRepair(t *testing.T) {
	var out bytes.Buffer
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  &scriptedQueryDraftAgent{draft: queryDraft{Query: "StormEvents | where Staet == 'WA' | take 5"}},
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(_ context.Context, _, _, csl, kind string, _ bool, _ map[string]any) (kustoResponse, error) {
			if kind == "query" {
				return kustoResponse{}, errors.New("execution should not run after query-plan validation failure")
			}
			if !strings.HasPrefix(csl, ".show queryplan <|") {
				return kustoResponse{}, errors.New("unexpected validation command: " + csl)
			}
			return kustoResponse{}, errors.New("Semantic error: Failed to resolve scalar expression named 'Staet'")
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--execute", "--validate-plan", "show", "WA", "storms"}); err != nil {
		t.Fatal(err)
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
	}
	if draft.Validation.QueryPlan == nil || draft.Validation.QueryPlan.Status != "failed" || !strings.Contains(draft.Validation.QueryPlan.Error, "Staet") {
		t.Fatalf("query-plan validation = %#v; want failed Staet error", draft.Validation.QueryPlan)
	}
	if draft.Validation.SafeForExecution || draft.Validation.Status != "failed" || !containsString(draft.Validation.Errors, "query-plan validation failed") {
		t.Fatalf("validation = %#v; want failed and not safe for execution", draft.Validation)
	}
	if draft.Execution.Executed || draft.Execution.Status != "blocked" || !strings.Contains(draft.Execution.Reason, "Execution Gate requested") {
		t.Fatalf("execution = %#v; want blocked without executing", draft.Execution)
	}
	if len(draft.RepairHistory) != 0 {
		t.Fatalf("repair history = %#v; want no Repair Pass without --repair", draft.RepairHistory)
	}
}

func TestAskRepairUsesPlanValidationErrorAndExecutesRepairedDraft(t *testing.T) {
	var out bytes.Buffer
	agent := &repairingQueryDraftAgent{
		generateDraft: queryDraft{Query: "StormEvents | where Staet == 'WA' | take 5"},
		repairDrafts: []queryDraft{{
			Query:       "StormEvents | where State == 'WA' | take 5",
			Assumptions: []string{"Corrected column name using Schema Context."},
		}},
	}
	planCalls := 0
	executionCalls := 0
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(_ context.Context, _, _, csl, kind string, _ bool, _ map[string]any) (kustoResponse, error) {
			switch kind {
			case "mgmt":
				planCalls++
				if !strings.HasPrefix(csl, ".show queryplan <|") {
					return kustoResponse{}, errors.New("unexpected validation command: " + csl)
				}
				if strings.Contains(csl, "Staet") {
					return kustoResponse{}, errors.New("Semantic error: Failed to resolve scalar expression named 'Staet'")
				}
				if !strings.Contains(csl, "State == 'WA'") {
					return kustoResponse{}, errors.New("unexpected repaired query-plan command: " + csl)
				}
				return makeKustoResponse([]kustoColumn{{ColumnName: "Plan", ColumnType: "string"}}, [][]any{{"validated"}}), nil
			case "query":
				executionCalls++
				if csl != "StormEvents | where State == 'WA' | take 5" {
					return kustoResponse{}, errors.New("unexpected execution query: " + csl)
				}
				return makeKustoResponse([]kustoColumn{{ColumnName: "EventType", ColumnType: "string"}}, [][]any{{"Hail"}}), nil
			default:
				return kustoResponse{}, errors.New("unexpected Kusto call kind: " + kind)
			}
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--execute", "--repair", "--max-repair-attempts", "2", "show", "WA", "storms"}); err != nil {
		t.Fatal(err)
	}
	if agent.generateCalls != 1 || len(agent.repairCalls) != 1 {
		t.Fatalf("generate calls = %d repair calls = %d; want one initial generation and one Repair Pass", agent.generateCalls, len(agent.repairCalls))
	}
	if planCalls != 2 || executionCalls != 1 {
		t.Fatalf("plan calls = %d execution calls = %d; want failed plan, repaired plan, then execution", planCalls, executionCalls)
	}
	req := agent.repairCalls[0]
	if req.PreviousQuery != "StormEvents | where Staet == 'WA' | take 5" || !strings.Contains(req.ValidationError, "Staet") {
		t.Fatalf("repair request = %#v; want previous query and validation error", req)
	}
	if len(req.SchemaContext) != 1 || len(req.SchemaContext[0].Tables) != 1 || len(req.SchemaContext[0].Tables[0].SampleRows) != 0 {
		t.Fatalf("repair Schema Context = %#v; want schema-only context without sample rows", req.SchemaContext)
	}
	if len(req.Examples) == 0 || req.Examples[0].Source != "bundled" || !req.DataDisclosure.Sent.Shots {
		t.Fatalf("repair examples/disclosure = %#v / %#v; want examples sent and disclosed as shots", req.Examples, req.DataDisclosure)
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
	}
	if draft.Query != "StormEvents | where State == 'WA' | take 5" {
		t.Fatalf("query = %q; want repaired draft", draft.Query)
	}
	if draft.Validation.QueryPlan == nil || draft.Validation.QueryPlan.Status != "passed" || !draft.Validation.SafeForExecution {
		t.Fatalf("validation = %#v; want passed query-plan validation", draft.Validation)
	}
	if len(draft.RepairHistory) != 1 || draft.RepairHistory[0].Attempt != 1 || !strings.Contains(draft.RepairHistory[0].ValidationError, "Staet") || draft.RepairHistory[0].OutputQuery != draft.Query {
		t.Fatalf("repair history = %#v; want first Repair Pass with validation error and repaired query", draft.RepairHistory)
	}
	if !draft.Execution.Executed || draft.Execution.Status != "succeeded" {
		t.Fatalf("execution = %#v; want repaired query executed", draft.Execution)
	}
	if draft.DataDisclosure.Sent.QueryResults || req.DataDisclosure.Sent.QueryResults {
		t.Fatal("query results were marked as sent to model provider")
	}
}

func TestAskRepairFailureReturnsLastDraftAndValidationErrorWithoutExecution(t *testing.T) {
	var out bytes.Buffer
	agent := &repairingQueryDraftAgent{
		generateDraft: queryDraft{Query: "StormEvents | where Staet == 'WA' | take 5"},
		repairDrafts:  []queryDraft{{Query: "StormEvents | where Staet2 == 'WA' | take 5"}},
	}
	planCalls := 0
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(_ context.Context, _, _, csl, kind string, _ bool, _ map[string]any) (kustoResponse, error) {
			if kind == "query" {
				return kustoResponse{}, errors.New("execution should not run when Repair Passes fail validation")
			}
			planCalls++
			if strings.Contains(csl, "Staet2") {
				return kustoResponse{}, errors.New("Semantic error: Failed to resolve scalar expression named 'Staet2'")
			}
			return kustoResponse{}, errors.New("Semantic error: Failed to resolve scalar expression named 'Staet'")
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--execute", "--repair", "show", "WA", "storms"}); err != nil {
		t.Fatal(err)
	}
	if len(agent.repairCalls) != defaultAskRepairMaxAttempts || planCalls != defaultAskRepairMaxAttempts+1 {
		t.Fatalf("repair calls = %d plan calls = %d; want one Repair Pass and validation of last draft", len(agent.repairCalls), planCalls)
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
	}
	if draft.Query != "StormEvents | where Staet2 == 'WA' | take 5" {
		t.Fatalf("query = %q; want last repaired draft", draft.Query)
	}
	if draft.Validation.QueryPlan == nil || draft.Validation.QueryPlan.Status != "failed" || !strings.Contains(draft.Validation.QueryPlan.Error, "Staet2") {
		t.Fatalf("query-plan validation = %#v; want last validation error", draft.Validation.QueryPlan)
	}
	if draft.Execution.Executed || draft.Execution.Status != "blocked" {
		t.Fatalf("execution = %#v; want blocked without execution", draft.Execution)
	}
	if len(draft.RepairHistory) != 1 || draft.RepairHistory[0].OutputQuery != draft.Query {
		t.Fatalf("repair history = %#v; want failed Repair Pass output recorded", draft.RepairHistory)
	}
	if !containsString(draft.Warnings, "Repair Pass maximum reached") {
		t.Fatalf("warnings = %#v; want maximum reached warning", draft.Warnings)
	}
}

func TestAskRepairEnforcesMaximumRepairAttempts(t *testing.T) {
	var out bytes.Buffer
	agent := &repairingQueryDraftAgent{
		generateDraft: queryDraft{Query: "StormEvents | where Bad0 == 'WA' | take 5"},
		repairDrafts: []queryDraft{
			{Query: "StormEvents | where Bad1 == 'WA' | take 5"},
			{Query: "StormEvents | where Bad2 == 'WA' | take 5"},
			{Query: "StormEvents | where Bad3 == 'WA' | take 5"},
		},
	}
	planCalls := 0
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(_ context.Context, _, _, csl, kind string, _ bool, _ map[string]any) (kustoResponse, error) {
			if kind == "query" {
				return kustoResponse{}, errors.New("execution should not run after max Repair Passes are exhausted")
			}
			planCalls++
			return kustoResponse{}, errors.New("Semantic error while validating: " + csl)
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--execute", "--repair", "--max-repair-attempts=2", "show", "WA", "storms"}); err != nil {
		t.Fatal(err)
	}
	if len(agent.repairCalls) != 2 {
		t.Fatalf("Repair Passes = %d; want strict max of 2", len(agent.repairCalls))
	}
	if planCalls != 3 {
		t.Fatalf("plan validation calls = %d; want initial plus two repaired drafts", planCalls)
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
	}
	if draft.Query != "StormEvents | where Bad2 == 'WA' | take 5" {
		t.Fatalf("query = %q; want last draft from second Repair Pass", draft.Query)
	}
	if len(draft.RepairHistory) != 2 || draft.RepairHistory[1].Attempt != 2 || strings.Contains(out.String(), "Bad3") {
		t.Fatalf("repair history/output = %#v; want exactly two Repair Passes and no third draft", draft.RepairHistory)
	}
	if draft.Execution.Executed {
		t.Fatalf("execution = %#v; want not executed", draft.Execution)
	}
}

func TestAskExecuteFailureReturnsQueryDraftWithMetadata(t *testing.T) {
	var out bytes.Buffer
	agent := &recordingQueryDraftAgent{}
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent:  agent,
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		executeHook: func(context.Context, string, string, string, string, bool, map[string]any) (kustoResponse, error) {
			return kustoResponse{}, errors.New("synthetic Kusto failure")
		},
		stdout: &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--execute", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatalf("unmarshal Query Draft: %v\n%s", err, out.String())
	}
	if draft.Query != "StormEvents | take 5" {
		t.Fatalf("query = %q; want generated query preserved", draft.Query)
	}
	if draft.Validation.Status != "passed" || !draft.Validation.SafeForExecution {
		t.Fatalf("validation = %#v; want generated validation metadata preserved", draft.Validation)
	}
	if !draft.Execution.Executed || draft.Execution.Status != "failed" || !strings.Contains(draft.Execution.Error, "synthetic Kusto failure") {
		t.Fatalf("execution = %#v; want explicit execution failure metadata", draft.Execution)
	}
	if !containsString(draft.Warnings, "Execution Gate attempted query execution") {
		t.Fatalf("warnings = %#v; want execution failure warning", draft.Warnings)
	}
}

func TestAskBlocksManagementCommandDraftWithValidationMetadata(t *testing.T) {
	draft := runAskWithDraft(t, queryDraft{
		Query:       ".show tables",
		Assumptions: []string{"The prompt might be asking for schema information."},
		Warnings:    []string{},
		ModelSafety: &queryDraftModelSafety{Classification: "safe", Reason: "Provider thought this was read-only."},
	})
	if draft.Validation.Status != "failed" {
		t.Fatalf("validation status = %q; want failed", draft.Validation.Status)
	}
	if draft.Validation.ReadOnly || draft.Validation.SafeForExecution {
		t.Fatalf("validation = %#v; want blocked non-read-only draft", draft.Validation)
	}
	if !containsString(draft.Validation.Errors, "management command") {
		t.Fatalf("validation errors = %#v; want management-command error", draft.Validation.Errors)
	}
	if draft.ModelSafety == nil || !draft.ModelSafety.Advisory || draft.ModelSafety.Classification != "safe" {
		t.Fatalf("model safety = %#v; want advisory safe classification preserved", draft.ModelSafety)
	}
	if !strings.Contains(draft.Execution.Reason, "blocked") {
		t.Fatalf("execution reason = %q; want validation block", draft.Execution.Reason)
	}
}

func TestAskBlocksObviousWriteCapableDraft(t *testing.T) {
	draft := runAskWithDraft(t, queryDraft{Query: "StormEvents | take 10 | into table SavedStormEvents"})
	if draft.Validation.Status != "failed" {
		t.Fatalf("validation status = %q; want failed", draft.Validation.Status)
	}
	if draft.Validation.ReadOnly || draft.Validation.SafeForExecution {
		t.Fatalf("validation = %#v; want unsafe draft blocked", draft.Validation)
	}
	if !containsString(draft.Validation.Errors, "write-capable or destructive") {
		t.Fatalf("validation errors = %#v; want destructive-shape error", draft.Validation.Errors)
	}
}

func TestAskBlocksUnsafeMultiStatementDraft(t *testing.T) {
	draft := runAskWithDraft(t, queryDraft{Query: "StormEvents | take 5; StormEvents | count"})
	if draft.Validation.Status != "failed" {
		t.Fatalf("validation status = %q; want failed", draft.Validation.Status)
	}
	if !containsString(draft.Validation.Errors, "multiple executable KQL statements") {
		t.Fatalf("validation errors = %#v; want unsafe multi-statement error", draft.Validation.Errors)
	}
}

func TestAskAllowsBoundedLetStatementDraft(t *testing.T) {
	draft := runAskWithDraft(t, queryDraft{
		Query:       "let min_time = ago(1d); StormEvents | where StartTime > min_time | take 5",
		Assumptions: []string{"Using StormEvents because it is the closest matching table in Schema Context."},
	})
	if draft.Validation.Status != "passed" || !draft.Validation.ReadOnly || !draft.Validation.SafeForExecution || !draft.Validation.Bounded {
		t.Fatalf("validation = %#v; want passed bounded read-only draft", draft.Validation)
	}
	if len(draft.Assumptions) == 0 {
		t.Fatal("non-blocking ambiguity assumption was not preserved")
	}
}

func TestAskWarnsAndBlocksUnboundedExploratoryDraft(t *testing.T) {
	draft := runAskWithDraft(t, queryDraft{Query: "StormEvents | where State == 'WA'"})
	if draft.Validation.Status != "warning" {
		t.Fatalf("validation status = %q; want warning", draft.Validation.Status)
	}
	if !draft.Validation.ReadOnly || draft.Validation.SafeForExecution || draft.Validation.Bounded {
		t.Fatalf("validation = %#v; want read-only but not executable unbounded draft", draft.Validation)
	}
	if !containsString(draft.Validation.Warnings, "explicit result bound") {
		t.Fatalf("validation warnings = %#v; want boundedness warning", draft.Validation.Warnings)
	}
	if !strings.Contains(draft.Execution.Reason, "validation warnings") {
		t.Fatalf("execution reason = %q; want warning block", draft.Execution.Reason)
	}
}

func TestAskCanReturnClarificationRequiredDraft(t *testing.T) {
	draft := runAskWithDraft(t, queryDraft{
		ClarificationRequired: true,
		ClarificationQuestion: "Which public sample table should be used?",
		Warnings:              []string{"The request could match more than one table."},
	})
	if !draft.ClarificationRequired {
		t.Fatal("clarification_required was not returned")
	}
	if draft.ClarificationQuestion == "" {
		t.Fatal("clarification question was not returned")
	}
	if draft.Query != "" {
		t.Fatalf("query = %q; want empty query for clarification-required response", draft.Query)
	}
	if draft.Validation.Status != "clarification_required" || draft.Validation.SafeForExecution {
		t.Fatalf("validation = %#v; want clarification-required block", draft.Validation)
	}
}

func TestAskSampleRowsRequireExplicitOptIn(t *testing.T) {
	for _, tc := range []struct {
		name               string
		args               []string
		wantSampleRows     bool
		wantDisclosureMode string
	}{
		{name: "default schema only", args: []string{"ask", "show", "storm", "events"}, wantSampleRows: false, wantDisclosureMode: dataDisclosureModeSchemaOnly},
		{name: "explicit sample opt-in", args: []string{"ask", "--include-samples", "show", "storm", "events"}, wantSampleRows: true, wantDisclosureMode: dataDisclosureModeSchemaAndSamples},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			agent := &recordingQueryDraftAgent{}
			schemaDiscoverer := &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()}
			s := &server{
				cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
				queryDraftAgent:  agent,
				schemaDiscoverer: schemaDiscoverer,
				stdout:           &out,
			}
			if err := s.loadKnownServices(); err != nil {
				t.Fatal(err)
			}
			if err := s.runCommand(context.Background(), tc.args); err != nil {
				t.Fatal(err)
			}
			if schemaDiscoverer.req.IncludeSampleRows != tc.wantSampleRows {
				t.Fatalf("IncludeSampleRows = %t; want %t", schemaDiscoverer.req.IncludeSampleRows, tc.wantSampleRows)
			}
			if len(agent.req.SchemaContext) != 1 || len(agent.req.SchemaContext[0].Tables) != 1 {
				t.Fatalf("agent schema context = %#v; want one fake table", agent.req.SchemaContext)
			}
			gotSamples := len(agent.req.SchemaContext[0].Tables[0].SampleRows) > 0
			if gotSamples != tc.wantSampleRows {
				t.Fatalf("sample rows sent to agent = %t; want %t", gotSamples, tc.wantSampleRows)
			}
			var draft queryDraft
			if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
				t.Fatal(err)
			}
			if draft.DataDisclosure.Mode != tc.wantDisclosureMode {
				t.Fatalf("disclosure mode = %q; want %q", draft.DataDisclosure.Mode, tc.wantDisclosureMode)
			}
			if draft.DataDisclosure.Sent.SampleRows != tc.wantSampleRows {
				t.Fatalf("disclosure sample_rows = %t; want %t", draft.DataDisclosure.Sent.SampleRows, tc.wantSampleRows)
			}
			if draft.DataDisclosure.Sent.QueryResults {
				t.Fatal("query results must not be sent to the model provider by default")
			}
		})
	}
}

func TestCompactSchemaCatalogFocusesPromptAndCapsTables(t *testing.T) {
	catalog := schemaCatalog{}
	for _, name := range []string{"Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta"} {
		catalog.Tables = append(catalog.Tables, queryDraftSchemaTable{
			Name:      name,
			DocString: "generic table",
			Columns: []queryDraftSchemaColumn{
				{Name: "Timestamp", Type: "datetime"},
				{Name: "Value", Type: "string"},
			},
		})
	}
	catalog.Tables = append(catalog.Tables, queryDraftSchemaTable{
		Name:      "StormEvents",
		DocString: "storm event facts",
		Columns: []queryDraftSchemaColumn{
			{Name: "StartTime", Type: "datetime"},
			{Name: "EventType", Type: "string"},
		},
	})
	catalog.Functions = []queryDraftSchemaFunction{
		{Name: "RecentStorms", DocString: "recent storm function", OutputColumns: []queryDraftSchemaColumn{{Name: "EventType", Type: "string"}}},
		{Name: "UnrelatedHelper", DocString: "not relevant"},
	}

	ctx := compactSchemaCatalog(catalog, "recent storm events")
	if len(ctx.Tables) != 1 || ctx.Tables[0].Name != "StormEvents" {
		t.Fatalf("focused tables = %#v; want only StormEvents", ctx.Tables)
	}
	if len(ctx.Functions) != 1 || ctx.Functions[0].Name != "RecentStorms" {
		t.Fatalf("focused functions = %#v; want RecentStorms", ctx.Functions)
	}
	if !ctx.Truncated {
		t.Fatal("focused schema context should report truncation when unrelated entities are omitted")
	}
}

func TestSchemaCatalogFromKustoResponseUsesSchemaAndFunctionMetadata(t *testing.T) {
	resp := makeKustoResponse(
		[]kustoColumn{
			{ColumnName: "EntityName", ColumnType: "string"},
			{ColumnName: "EntityType", ColumnType: "string"},
			{ColumnName: "DocString", ColumnType: "string"},
			{ColumnName: "CslInputSchema", ColumnType: "string"},
			{ColumnName: "CslOutputSchema", ColumnType: "string"},
		},
		[][]any{
			{"StormEvents", "Table", "Public sample storm events.", "StormEvents(StartTime:datetime, State:string, EventType:string)", ""},
			{"RecentStorms", "Function", "Recent storms helper.", "(state:string)", "RecentStorms(StartTime:datetime, State:string)"},
		},
	)

	catalog := schemaCatalogFromKustoResponse(resp)
	if len(catalog.Tables) != 1 || catalog.Tables[0].Name != "StormEvents" || catalog.Tables[0].DocString == "" {
		t.Fatalf("tables = %#v; want StormEvents with docstring", catalog.Tables)
	}
	if got := catalog.Tables[0].Columns; len(got) != 3 || got[0].Name != "StartTime" || got[0].Type != "datetime" {
		t.Fatalf("table columns = %#v; want parsed Kusto column schema", got)
	}
	if len(catalog.Functions) != 1 || catalog.Functions[0].Name != "RecentStorms" || catalog.Functions[0].InputSchema != "(state:string)" {
		t.Fatalf("functions = %#v; want RecentStorms metadata", catalog.Functions)
	}
	if got := catalog.Functions[0].OutputColumns; len(got) != 2 || got[1].Name != "State" || got[1].Type != "string" {
		t.Fatalf("function output columns = %#v; want parsed output schema", got)
	}
}

func TestFakeQueryDraftAgentEscapesPrompt(t *testing.T) {
	draft, err := fakeQueryDraftAgent{}.GenerateQueryDraft(context.Background(), queryDraftRequest{
		Target: queryDraftTarget{ClusterURI: "https://help.kusto.windows.net", Database: "Samples"},
		Prompt: "can't find storms",
	})
	if err != nil {
		t.Fatal(err)
	}
	if draft.Query != "search 'can''t find storms'\n| take 10" {
		t.Fatalf("query = %q", draft.Query)
	}
}

func TestAskDefaultsToFakeModelProvider(t *testing.T) {
	var out bytes.Buffer
	s := &server{
		cfg:              config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		stdout:           &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatal(err)
	}
	if draft.Query != "search 'show storm events'\n| take 10" {
		t.Fatalf("query = %q; want deterministic fake-provider query", draft.Query)
	}
	if len(draft.Assumptions) != 2 || !strings.Contains(draft.Assumptions[1], "No real model provider") {
		t.Fatalf("assumptions = %#v; want fake provider disclosure", draft.Assumptions)
	}
}

func TestAskOpenAICompatibleModelProviderProducesQueryDraft(t *testing.T) {
	const secret = "sk-test-model-secret"
	t.Setenv("KUSTO_TEST_MODEL_KEY", secret)

	sawRequest := false
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest = true
		if r.Method != http.MethodPost {
			t.Error("model provider should use POST")
		}
		if r.Header.Get("Authorization") != "Bearer "+secret {
			t.Error("unexpected Authorization header")
		}
		var got openAIChatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error("failed to decode model provider request")
			return
		}
		if got.Model != "test-model" {
			t.Errorf("model = %q; want test-model", got.Model)
		}
		if got.ResponseFormat.Type != "json_schema" || got.ResponseFormat.JSONSchema.Name != "kusto_query_draft" {
			t.Errorf("response_format = %#v; want query draft JSON schema", got.ResponseFormat)
		}
		requestText, err := json.Marshal(got)
		if err != nil {
			t.Error("failed to inspect model provider request")
			return
		}
		if strings.Contains(string(requestText), secret) {
			t.Error("model provider request body leaked API key")
		}
		if !strings.Contains(got.Messages[len(got.Messages)-1].Content, "StormEvents") {
			t.Error("model provider request did not include Schema Context")
		}
		if !strings.Contains(got.Messages[len(got.Messages)-1].Content, "\"examples\"") || !strings.Contains(got.Messages[len(got.Messages)-1].Content, "recent_rows") {
			t.Error("model provider request did not include Query Draft examples")
		}

		w.Header().Set("Content-Type", "application/json")
		content := `{"query":"StormEvents | take 5","clarification_required":false,"clarification_question":"","assumptions":["Using the public sample StormEvents table."],"warnings":["Review before execution."],"model_safety":{"classification":"safe","reason":"Read-only query draft."}}`
		if err := json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": content}}},
		}); err != nil {
			t.Error("failed to write model provider response")
		}
	}))
	defer modelServer.Close()

	var out bytes.Buffer
	s := &server{
		cfg: config{
			serviceURI:     "https://help.kusto.windows.net",
			database:       "Samples",
			output:         "json",
			modelProvider:  "openai-compatible",
			modelEndpoint:  modelServer.URL,
			modelName:      "test-model",
			modelAPIKeyEnv: "KUSTO_TEST_MODEL_KEY",
		},
		hc:               modelServer.Client(),
		schemaDiscoverer: &fakeSchemaDiscoverer{ctx: fakeStormSchemaContext()},
		stdout:           &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if !sawRequest {
		t.Fatal("model provider was not called")
	}
	if strings.Contains(out.String(), secret) {
		t.Fatal("ask output leaked API key")
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatal(err)
	}
	if draft.Query != "StormEvents | take 5" {
		t.Fatalf("query = %q; want model provider query", draft.Query)
	}
	if draft.ModelSafety == nil || draft.ModelSafety.Classification != "safe" || !draft.ModelSafety.Advisory {
		t.Fatalf("model safety = %#v; want advisory safe classification", draft.ModelSafety)
	}
	if draft.Validation.Status != "passed" || !draft.Validation.ReadOnly {
		t.Fatalf("validation = %#v; want independent read-only validation to pass", draft.Validation)
	}
}

func TestOpenAICompatibleModelProviderConfigErrorsDoNotLeakSecrets(t *testing.T) {
	const secret = "sk-test-config-secret"
	t.Setenv("KUSTO_TEST_MODEL_KEY", secret)
	t.Setenv("KUSTO_TEST_MISSING_MODEL_KEY", "")

	_, err := newConfiguredModelProvider(config{modelProvider: "openai-compatible", modelAPIKeyEnv: "KUSTO_TEST_MODEL_KEY"}, nil)
	if err == nil || !strings.Contains(err.Error(), "model is required") {
		t.Fatal("expected missing model configuration error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("missing model error leaked API key")
	}

	_, err = newConfiguredModelProvider(config{modelProvider: "openai-compatible", modelName: "test-model", modelAPIKeyEnv: "KUSTO_TEST_MISSING_MODEL_KEY"}, nil)
	if err == nil || !strings.Contains(err.Error(), "KUSTO_TEST_MISSING_MODEL_KEY") {
		t.Fatal("expected missing API key environment variable error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("missing API key error leaked another API key")
	}
}

func TestOpenAICompatibleModelProviderMalformedStructuredOutput(t *testing.T) {
	const secret = "sk-test-malformed-secret"
	t.Setenv("KUSTO_TEST_MODEL_KEY", secret)
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		content := `{"assumptions":[],"warnings":[]}`
		if err := json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": content}}},
		}); err != nil {
			t.Error("failed to write model provider response")
		}
	}))
	defer modelServer.Close()

	provider, err := newOpenAICompatibleModelProvider(config{
		modelProvider:  "openai-compatible",
		modelEndpoint:  modelServer.URL,
		modelName:      "test-model",
		modelAPIKeyEnv: "KUSTO_TEST_MODEL_KEY",
	}, modelServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.GenerateQueryDraft(context.Background(), queryDraftRequest{Prompt: "show storm events"})
	if err == nil || !strings.Contains(err.Error(), "malformed structured output") {
		t.Fatal("expected malformed structured output error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("malformed output error leaked API key")
	}
}

func TestOpenAICompatibleModelProviderHTTPErrorRedactsAPIKey(t *testing.T) {
	const secret = "sk-test-http-secret"
	t.Setenv("KUSTO_TEST_MODEL_KEY", secret)
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"bad token sk-test-http-secret"}}`))
	}))
	defer modelServer.Close()

	provider, err := newOpenAICompatibleModelProvider(config{
		modelProvider:  "openai-compatible",
		modelEndpoint:  modelServer.URL,
		modelName:      "test-model",
		modelAPIKeyEnv: "KUSTO_TEST_MODEL_KEY",
	}, modelServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.GenerateQueryDraft(context.Background(), queryDraftRequest{Prompt: "show storm events"})
	if err == nil {
		t.Fatal("expected HTTP error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("HTTP error leaked API key")
	}
	if !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatal("HTTP error did not redact API key")
	}
}

func TestAskRejectsFallbackDatabaseAsTargetAndDoesNotCallAgent(t *testing.T) {
	var out bytes.Buffer
	agent := &recordingQueryDraftAgent{}
	s := &server{
		cfg:             config{serviceURI: "https://help.kusto.windows.net", database: defaultKustoDB, output: "json"},
		queryDraftAgent: agent,
		stdout:          &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	err := s.runCommand(context.Background(), []string{"ask", "show", "storm", "events"})
	if err == nil || !strings.Contains(err.Error(), "ask requires a Target database") {
		t.Fatalf("unexpected error: %v", err)
	}
	if agent.called {
		t.Fatal("Query Draft Agent was called before Target resolution failed")
	}
	if out.Len() != 0 {
		t.Fatalf("failed ask wrote output: %q", out.String())
	}
}

func TestAskSelectsTargetByAliasFromCatalog(t *testing.T) {
	var out bytes.Buffer
	agent := &recordingQueryDraftAgent{}
	s := &server{
		cfg: config{
			knownServices: `[
				{"alias":"samples","service_uri":"https://help.kusto.windows.net","default_database":"Samples","description":"Public sample data"},
				{"alias":"samples-alt","service_uri":"https://help.kusto.windows.net:443","default_database":"Samples","description":"Alternate test target"}
			]`,
			output: "json",
		},
		queryDraftAgent: agent,
		stdout:          &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--target", "samples", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if !agent.called {
		t.Fatal("fake Query Draft Agent was not called")
	}
	if agent.req.Target.ClusterURI != "https://help.kusto.windows.net" || agent.req.Target.Database != "Samples" {
		t.Fatalf("target = %#v; want samples alias target", agent.req.Target)
	}
	var draft queryDraft
	if err := json.Unmarshal(out.Bytes(), &draft); err != nil {
		t.Fatal(err)
	}
	if draft.Target.ClusterURI != "https://help.kusto.windows.net" || draft.Target.Database != "Samples" {
		t.Fatalf("output target = %#v; want resolved samples target", draft.Target)
	}
}

func TestAskMultipleTargetsWithoutSelectionFailsAndDoesNotInferFromPrompt(t *testing.T) {
	var out bytes.Buffer
	agent := &recordingQueryDraftAgent{}
	s := &server{
		cfg: config{
			knownServices: `[
				{"alias":"samples","service_uri":"https://help.kusto.windows.net","default_database":"Samples"},
				{"alias":"samples-alt","service_uri":"https://help.kusto.windows.net:443","default_database":"Samples"}
			]`,
			output: "json",
		},
		queryDraftAgent: agent,
		stdout:          &out,
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	err := s.runCommand(context.Background(), []string{"ask", "use", "the", "samples", "target"})
	if err == nil {
		t.Fatal("expected multiple Target Catalog entries to require explicit selection")
	}
	for _, want := range []string{"multiple targets are configured", "samples:", "samples-alt:"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err.Error(), want)
		}
	}
	if agent.called {
		t.Fatal("Query Draft Agent was called despite ambiguous Target Catalog")
	}
	if out.Len() != 0 {
		t.Fatalf("failed ask wrote output: %q", out.String())
	}
}

func TestAskSupportsNameAliasAndLegacyServiceField(t *testing.T) {
	agent := &recordingQueryDraftAgent{}
	s := &server{
		cfg: config{
			knownServices: `[{"name":"samples","service":"https://help.kusto.windows.net","default_database":"Samples"}]`,
			targetAlias:   "samples",
			output:        "json",
		},
		queryDraftAgent: agent,
		stdout:          &bytes.Buffer{},
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if !agent.called {
		t.Fatal("fake Query Draft Agent was not called")
	}
	if agent.req.Target.ClusterURI != "https://help.kusto.windows.net" || agent.req.Target.Database != "Samples" {
		t.Fatalf("target = %#v; want target resolved through name/service compatibility", agent.req.Target)
	}
}

func TestDefaultDatabaseFallbackRemainsForDirectResolution(t *testing.T) {
	s := &server{cfg: config{database: defaultKustoDB, knownServices: `[{"service_uri":"https://help.kusto.windows.net"}]`}}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if got := s.defaultDatabaseFor("https://help.kusto.windows.net", ""); got != defaultKustoDB {
		t.Fatalf("defaultDatabaseFor fallback = %q; want %q", got, defaultKustoDB)
	}
}

func TestAskLocalServiceDatabaseOverridesConfiguredTargetAlias(t *testing.T) {
	agent := &recordingQueryDraftAgent{}
	s := &server{
		cfg:             config{targetAlias: "missing", output: "json"},
		queryDraftAgent: agent,
		stdout:          &bytes.Buffer{},
	}
	if err := s.loadKnownServices(); err != nil {
		t.Fatal(err)
	}
	if err := s.runCommand(context.Background(), []string{"ask", "--service-uri", "https://help.kusto.windows.net", "--database", "Samples", "show", "storm", "events"}); err != nil {
		t.Fatal(err)
	}
	if !agent.called {
		t.Fatal("fake Query Draft Agent was not called")
	}
	if agent.req.Target.ClusterURI != "https://help.kusto.windows.net" || agent.req.Target.Database != "Samples" {
		t.Fatalf("target = %#v; want local service/database target", agent.req.Target)
	}
}
