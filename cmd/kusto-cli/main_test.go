package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		Query:          "StormEvents | take 5",
		Assumptions:    []string{"Using the public sample StormEvents table."},
		Warnings:       []string{"Fake agent response for deterministic tests."},
		SchemaContext:  req.SchemaContext,
		DataDisclosure: req.DataDisclosure,
	}, nil
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
      "shots": false,
      "sample_rows": false,
      "query_results": false
    }
  },
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
	if strings.Contains(out.String(), "Hail") {
		t.Fatal("ask output included raw sample row values without explicit opt-in")
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

		w.Header().Set("Content-Type", "application/json")
		content := `{"query":"StormEvents | take 5","assumptions":["Using the public sample StormEvents table."],"warnings":["Review before execution."],"model_safety":{"classification":"safe","reason":"Read-only query draft."}}`
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
