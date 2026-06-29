package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type recordingQueryDraftAgent struct {
	called bool
	req    queryDraftRequest
}

func (a *recordingQueryDraftAgent) GenerateQueryDraft(_ context.Context, req queryDraftRequest) (queryDraft, error) {
	a.called = true
	a.req = req
	return queryDraft{
		Query:       "StormEvents | take 5",
		Assumptions: []string{"Using the public sample StormEvents table."},
		Warnings:    []string{"Fake agent response for deterministic tests."},
		SchemaContext: []queryDraftSchemaContext{
			{Source: "fake-test", Entities: []string{"StormEvents"}},
		},
	}, nil
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
	s := &server{
		cfg:             config{serviceURI: "https://help.kusto.windows.net", database: "Samples", output: "json"},
		queryDraftAgent: agent,
		stdout:          &out,
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
	if agent.req.Prompt != "show recent storm events" {
		t.Fatalf("prompt = %q; want joined natural-language prompt", agent.req.Prompt)
	}
	if agent.req.Target.ClusterURI != "https://help.kusto.windows.net" || agent.req.Target.Database != "Samples" {
		t.Fatalf("target = %#v; want public sample target", agent.req.Target)
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
  "schema_context": [
    {
      "source": "fake-test",
      "entities": [
        "StormEvents"
      ]
    }
  ],
  "validation": {
    "status": "passed",
    "read_only": true,
    "checks": [
      {
        "name": "query_not_empty",
        "passed": true
      },
      {
        "name": "not_management_command",
        "passed": true
      }
    ],
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
