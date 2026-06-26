package main

import (
	"encoding/json"
	"strings"
	"testing"
)

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
