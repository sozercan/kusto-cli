package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sozercan/kusto-cli/internal/version"
)

const (
	defaultProtocolVer   = "2025-06-18"
	defaultKustoDB       = "NetDefaultDB"
	defaultKustoResource = "https://kusto.kusto.windows.net"

	dataDisclosureModeSchemaOnly       = "schema-only"
	dataDisclosureModeSchemaAndSamples = "schema-and-sample-rows"

	defaultSchemaContextSampleRows = 3
	defaultAskExecutionMaxRecords  = 100
	defaultAskRepairMaxAttempts    = 1
	defaultQueryDraftShotsLimit    = 5
	maxAskRepairMaxAttempts        = 5
	maxQueryDraftShotsLimit        = 20
	maxBundledQueryDraftExamples   = 3
	maxSchemaContextTables         = 5
	maxSchemaContextFunctions      = 3
	maxSchemaContextColumns        = 24
)

type config struct {
	serviceURI         string
	database           string
	knownServices      string
	targetAlias        string
	databaseConfigured bool
	serviceURIFlag     bool
	targetAliasFlag    bool
	tokenEnv           string
	authMode           string
	tenant             string
	userAgent          string
	timeout            time.Duration
	output             string
	modelProvider      string
	modelEndpoint      string
	modelName          string
	modelAPIKeyEnv     string
	allowWrite         bool
	dryRun             bool
	noInput            bool
	force              bool
	debug              bool
	printVersion       bool
	args               []string
}

type server struct {
	cfg              config
	hc               *http.Client
	auth             *tokenProvider
	queryDraftAgent  queryDraftAgent
	schemaDiscoverer schemaDiscoverer
	executeHook      kustoExecuteFunc
	stdout           io.Writer
	knownServices    []KustoServiceConfig
	defaultSvc       *KustoServiceConfig
}

type tokenProvider struct {
	mode     string
	tokenEnv string
	tenant   string
	debug    bool
	mu       sync.Mutex
	token    string
	expires  time.Time
}

type KustoServiceConfig struct {
	Alias           string   `json:"alias,omitempty"`
	Name            string   `json:"name,omitempty"`
	Aliases         []string `json:"aliases,omitempty"`
	ServiceURI      string   `json:"service_uri"`
	Service         string   `json:"service,omitempty"`
	DefaultDatabase string   `json:"default_database,omitempty"`
	Description     string   `json:"description,omitempty"`
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type kustoColumn struct {
	ColumnName string `json:"ColumnName"`
	ColumnType string `json:"ColumnType"`
	DataType   string `json:"DataType,omitempty"`
}

type kustoFrame struct {
	FrameType    string            `json:"FrameType"`
	TableID      int               `json:"TableId,omitempty"`
	TableKind    string            `json:"TableKind,omitempty"`
	TableName    string            `json:"TableName,omitempty"`
	Columns      []kustoColumn     `json:"Columns,omitempty"`
	Rows         [][]any           `json:"Rows,omitempty"`
	HasErrors    bool              `json:"HasErrors,omitempty"`
	OneAPIErrors []json.RawMessage `json:"OneApiErrors,omitempty"`
}

type kustoResponse struct {
	Data struct {
		Columns []kustoColumn `json:"columns"`
		Rows    [][]any       `json:"rows"`
	} `json:"data"`
	Format string `json:"format"`
}

type queryDraftTarget struct {
	ClusterURI string `json:"cluster_uri"`
	Database   string `json:"database"`
}

type queryDraftRequest struct {
	Target         queryDraftTarget          `json:"target"`
	Prompt         string                    `json:"prompt"`
	SchemaContext  []queryDraftSchemaContext `json:"schema_context"`
	Examples       []queryDraftExample       `json:"examples,omitempty"`
	DataDisclosure queryDraftDataDisclosure  `json:"data_disclosure_policy"`
}

type queryDraft struct {
	Format                string                    `json:"format"`
	Target                queryDraftTarget          `json:"target"`
	Prompt                string                    `json:"prompt"`
	Query                 string                    `json:"query"`
	ClarificationRequired bool                      `json:"clarification_required,omitempty"`
	ClarificationQuestion string                    `json:"clarification_question,omitempty"`
	Assumptions           []string                  `json:"assumptions"`
	Warnings              []string                  `json:"warnings"`
	Examples              []queryDraftExample       `json:"examples,omitempty"`
	SchemaContext         []queryDraftSchemaContext `json:"schema_context"`
	DataDisclosure        queryDraftDataDisclosure  `json:"data_disclosure_policy"`
	Validation            queryDraftValidation      `json:"validation"`
	Execution             queryDraftExecution       `json:"execution"`
	RepairHistory         []queryDraftRepairPass    `json:"repair_history,omitempty"`
	ModelSafety           *queryDraftModelSafety    `json:"model_safety,omitempty"`
}

type queryDraftRepairPass struct {
	Attempt         int    `json:"attempt"`
	Trigger         string `json:"trigger"`
	InputQuery      string `json:"input_query"`
	ValidationError string `json:"validation_error"`
	OutputQuery     string `json:"output_query,omitempty"`
	Status          string `json:"status"`
	Error           string `json:"error,omitempty"`
}

type queryDraftRepairRequest struct {
	Target            queryDraftTarget          `json:"target"`
	Prompt            string                    `json:"prompt"`
	SchemaContext     []queryDraftSchemaContext `json:"schema_context"`
	Examples          []queryDraftExample       `json:"examples,omitempty"`
	DataDisclosure    queryDraftDataDisclosure  `json:"data_disclosure_policy"`
	PreviousQuery     string                    `json:"previous_query"`
	ValidationError   string                    `json:"validation_error"`
	ValidationErrors  []string                  `json:"validation_errors"`
	RepairAttempt     int                       `json:"repair_attempt"`
	MaxRepairAttempts int                       `json:"max_repair_attempts"`
}

type queryDraftModelSafety struct {
	Classification string `json:"classification,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Advisory       bool   `json:"advisory,omitempty"`
}

type queryDraftExample struct {
	Source string `json:"source"`
	Name   string `json:"name,omitempty"`
	Intent string `json:"intent"`
	Query  string `json:"query"`
}

type queryDraftSchemaContext struct {
	Source    string                     `json:"source"`
	Entities  []string                   `json:"entities"`
	Tables    []queryDraftSchemaTable    `json:"tables,omitempty"`
	Functions []queryDraftSchemaFunction `json:"functions,omitempty"`
	Truncated bool                       `json:"truncated,omitempty"`
	Warnings  []string                   `json:"warnings,omitempty"`
}

type queryDraftSchemaTable struct {
	EntityType string                   `json:"-"`
	Name       string                   `json:"name"`
	DocString  string                   `json:"docstring,omitempty"`
	Columns    []queryDraftSchemaColumn `json:"columns"`
	SampleRows []map[string]any         `json:"sample_rows,omitempty"`
}

type queryDraftSchemaColumn struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	DocString string `json:"docstring,omitempty"`
}

type queryDraftSchemaFunction struct {
	Name          string                   `json:"name"`
	DocString     string                   `json:"docstring,omitempty"`
	InputSchema   string                   `json:"input_schema,omitempty"`
	OutputColumns []queryDraftSchemaColumn `json:"output_columns,omitempty"`
}

type queryDraftDataDisclosure struct {
	Mode string                       `json:"mode"`
	Sent queryDraftDataDisclosureSent `json:"sent_to_model_provider"`
}

type queryDraftDataDisclosureSent struct {
	Schema       bool `json:"schema"`
	Docstrings   bool `json:"docstrings"`
	Shots        bool `json:"shots"`
	SampleRows   bool `json:"sample_rows"`
	QueryResults bool `json:"query_results"`
}

type queryDraftValidation struct {
	Status           string                      `json:"status"`
	ReadOnly         bool                        `json:"read_only"`
	Bounded          bool                        `json:"bounded"`
	SafeForExecution bool                        `json:"safe_for_execution"`
	QueryPlan        *queryDraftPlanValidation   `json:"query_plan,omitempty"`
	Checks           []queryDraftValidationCheck `json:"checks"`
	Warnings         []string                    `json:"warnings"`
	Errors           []string                    `json:"errors"`
}

type queryDraftPlanValidation struct {
	Requested bool   `json:"requested"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Message   string `json:"message,omitempty"`
}

type queryDraftValidationCheck struct {
	Name     string `json:"name"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity,omitempty"`
	Message  string `json:"message,omitempty"`
}

type queryDraftExecution struct {
	Executed   bool           `json:"executed"`
	Reason     string         `json:"reason"`
	Status     string         `json:"status,omitempty"`
	MaxRecords int            `json:"max_records,omitempty"`
	Error      string         `json:"error,omitempty"`
	Result     *kustoResponse `json:"result,omitempty"`
}

type kustoExecuteFunc func(ctx context.Context, clusterURI, database, csl, kind string, readonly bool, crp map[string]any) (kustoResponse, error)

type queryDraftAgent interface {
	GenerateQueryDraft(context.Context, queryDraftRequest) (queryDraft, error)
}

type queryDraftRepairer interface {
	RepairQueryDraft(context.Context, queryDraftRepairRequest) (queryDraft, error)
}

type fakeQueryDraftAgent struct{}

type schemaDiscoverer interface {
	DiscoverSchemaContext(context.Context, schemaDiscoveryRequest) (queryDraftSchemaContext, error)
}

type schemaDiscoveryRequest struct {
	Target            queryDraftTarget
	Prompt            string
	IncludeSampleRows bool
	SampleRowLimit    int
}

type emptySchemaDiscoverer struct{}

type kustoSchemaDiscoverer struct {
	server *server
}

func main() {
	cfg := parseFlags()
	if cfg.printVersion {
		fmt.Printf("kusto-cli %s (%s)\n", version.Version, version.Commit)
		return
	}
	log.SetOutput(os.Stderr)
	log.SetFlags(0)
	if err := run(context.Background(), cfg); err != nil {
		log.Fatalf("kusto-cli: %v", err)
	}
}

func parseFlags() config {
	cfg := config{}
	targetAliasDefault := firstNonEmpty(os.Getenv("KUSTO_TARGET"), os.Getenv("KUSTO_TARGET_ALIAS"))
	flag.StringVar(&cfg.serviceURI, "service-uri", firstNonEmpty(os.Getenv("KUSTO_SERVICE_URI"), ""), "default Kusto cluster URI")
	flag.StringVar(&cfg.serviceURI, "service-url", firstNonEmpty(os.Getenv("KUSTO_SERVICE_URI"), ""), "alias for --service-uri")
	flag.StringVar(&cfg.database, "database", firstNonEmpty(os.Getenv("KUSTO_SERVICE_DEFAULT_DB"), defaultKustoDB), "default database")
	flag.StringVar(&cfg.knownServices, "known-services", os.Getenv("KUSTO_KNOWN_SERVICES"), "JSON array of known services")
	flag.StringVar(&cfg.targetAlias, "target", targetAliasDefault, "Target Catalog alias for ask")
	flag.StringVar(&cfg.targetAlias, "target-alias", targetAliasDefault, "alias for --target")
	flag.StringVar(&cfg.tokenEnv, "token-env", "KUSTO_ACCESS_TOKEN", "environment variable containing a Kusto bearer token")
	flag.StringVar(&cfg.authMode, "auth", "auto", "auth mode: auto, env, azcli, none")
	flag.StringVar(&cfg.tenant, "tenant", "", "optional Azure tenant id for az CLI token acquisition")
	flag.StringVar(&cfg.userAgent, "user-agent", "kusto-cli/"+version.Version, "User-Agent sent to Kusto")
	flag.DurationVar(&cfg.timeout, "timeout", 90*time.Second, "Kusto HTTP timeout")
	flag.StringVar(&cfg.output, "output", firstNonEmpty(os.Getenv("KUSTO_OUTPUT"), "json"), "Direct command output: json, table, or tsv")
	flag.StringVar(&cfg.output, "o", firstNonEmpty(os.Getenv("KUSTO_OUTPUT"), "json"), "Alias for --output")
	flag.StringVar(&cfg.modelProvider, "model-provider", os.Getenv("KUSTO_MODEL_PROVIDER"), "ask model provider: fake or openai-compatible")
	flag.StringVar(&cfg.modelEndpoint, "model-endpoint", os.Getenv("KUSTO_MODEL_ENDPOINT"), "OpenAI-compatible chat completions endpoint for ask")
	flag.StringVar(&cfg.modelName, "model", os.Getenv("KUSTO_MODEL"), "model name for ask when using a real model provider")
	flag.StringVar(&cfg.modelAPIKeyEnv, "model-api-key-env", firstNonEmpty(os.Getenv("KUSTO_MODEL_API_KEY_ENV"), "OPENAI_API_KEY"), "environment variable containing the model provider API key")
	flag.BoolVar(&cfg.allowWrite, "allow-write", false, "Allow destructive Kusto operations")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "Preview write operations without executing")
	flag.BoolVar(&cfg.noInput, "no-input", false, "Never prompt; fail instead (reserved for scripting consistency)")
	flag.BoolVar(&cfg.force, "force", false, "Skip confirmation prompts (reserved for scripting consistency)")
	flag.BoolVar(&cfg.debug, "debug", false, "write debug logs to stderr")
	flag.BoolVar(&cfg.printVersion, "version", false, "print version and exit")
	flag.Parse()
	cfg.args = flag.Args()
	visited := visitedFlags()
	cfg.serviceURIFlag = visited["service-uri"] || visited["service-url"]
	cfg.targetAliasFlag = visited["target"] || visited["target-alias"]
	cfg.databaseConfigured = visited["database"] || os.Getenv("KUSTO_SERVICE_DEFAULT_DB") != ""
	applyFileConfig(&cfg, visited)
	return cfg
}

func run(ctx context.Context, cfg config) error {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSHandshakeTimeout = cfg.timeout
	s := &server{
		cfg:    cfg,
		hc:     &http.Client{Timeout: cfg.timeout, Transport: transport},
		auth:   &tokenProvider{mode: cfg.authMode, tokenEnv: cfg.tokenEnv, tenant: cfg.tenant, debug: cfg.debug},
		stdout: os.Stdout,
	}
	s.schemaDiscoverer = kustoSchemaDiscoverer{server: s}
	if err := s.loadKnownServices(); err != nil {
		return err
	}
	if len(cfg.args) == 0 || cfg.args[0] == "serve" {
		return s.serveStdio(ctx, os.Stdin, os.Stdout)
	}
	return s.runCommand(ctx, cfg.args)
}

func (s *server) loadKnownServices() error {
	add := func(svc KustoServiceConfig) {
		if svc.ServiceURI == "" {
			svc.ServiceURI = svc.Service
		}
		svc.Alias = strings.TrimSpace(svc.Alias)
		svc.Name = strings.TrimSpace(svc.Name)
		for i := range svc.Aliases {
			svc.Aliases[i] = strings.TrimSpace(svc.Aliases[i])
		}
		svc.ServiceURI = strings.TrimSpace(svc.ServiceURI)
		svc.DefaultDatabase = strings.TrimSpace(svc.DefaultDatabase)
		if svc.ServiceURI == "" {
			return
		}
		if svc.DefaultDatabase == "" && s.cfg.hasConfiguredDatabase() {
			svc.DefaultDatabase = strings.TrimSpace(s.cfg.database)
		}
		s.knownServices = append(s.knownServices, svc)
		if s.defaultSvc == nil {
			copy := svc
			s.defaultSvc = &copy
		}
	}
	if s.cfg.serviceURI != "" {
		defaultDatabase := ""
		if s.cfg.hasConfiguredDatabase() {
			defaultDatabase = strings.TrimSpace(s.cfg.database)
		}
		add(KustoServiceConfig{ServiceURI: s.cfg.serviceURI, DefaultDatabase: defaultDatabase, Description: "Default"})
	}
	if strings.TrimSpace(s.cfg.knownServices) != "" {
		var svcs []KustoServiceConfig
		if err := json.Unmarshal([]byte(s.cfg.knownServices), &svcs); err != nil {
			return fmt.Errorf("parse --known-services: %w", err)
		}
		for _, svc := range svcs {
			add(svc)
		}
	}
	return nil
}

func (s *server) runCommand(ctx context.Context, args []string) error {
	switch args[0] {
	case "ask":
		return s.runAskCommand(ctx, args[1:])
	case "query":
		return s.runQueryCommand(ctx, args[1:])
	case "command":
		return s.runMgmtCommand(ctx, args[1:])
	case "databases", "database":
		return s.runDatabaseCommand(ctx, args[1:])
	case "tables", "table":
		return s.runTableCommand(ctx, args[1:])
	case "entities", "entity":
		return s.runEntityCommand(ctx, args[1:])
	case "services", "service":
		return s.runServiceCommand(ctx, args[1:])
	case "deeplink":
		return s.runDeeplinkCommand(ctx, args[1:])
	case "queryplan":
		return s.runQueryPlanCommand(ctx, args[1:])
	case "diagnostics":
		return s.runDiagnosticsCommand(ctx, args[1:])
	case "api":
		return s.runAPICommand(ctx, args[1:])
	case "tools": // backwards-compatible alias for api tools
		return s.runAPICommand(ctx, []string{"tools"})
	case "schema": // backwards-compatible alias for api schema
		return s.runAPICommand(ctx, append([]string{"schema"}, args[1:]...))
	case "call": // backwards-compatible alias for api call
		return s.runAPICommand(ctx, append([]string{"call"}, args[1:]...))
	case "auth":
		return s.runAuthCommand(ctx, args[1:])
	case "config":
		return runConfigCommand("kusto-cli", args[1:])
	case "completion":
		return runCompletionCommand("kusto-cli", args[1:])
	case "help":
		printKustoUsage(os.Stdout)
		return nil
	default:
		return fmt.Errorf("unknown command %q (run 'kusto-cli help')", args[0])
	}
}

type askTargetSelection struct {
	Alias             string
	ServiceURI        string
	Database          string
	ServiceURISet     bool
	DatabaseSet       bool
	IncludeSampleRows bool
	Execute           bool
	ValidatePlan      bool
	ValidatePlanSet   bool
	Repair            bool
	RepairSet         bool
	MaxRecords        int
	MaxRepairAttempts int
	MaxRepairSet      bool
	ShotsTableName    string
	ShotsTableSet     bool
	ShotsLimit        int
	ShotsLimitSet     bool
}

type askTargetCandidate struct {
	ClusterURI  string
	Database    string
	Aliases     []string
	Description string
}

func (s *server) runAskCommand(ctx context.Context, args []string) error {
	selection, prompt, err := parseAskArgs(args)
	if err != nil {
		return err
	}
	selection, err = applyConfiguredAskDefaults(selection)
	if err != nil {
		return err
	}
	target, err := s.queryDraftTarget(selection)
	if err != nil {
		return err
	}
	schemaContext, schemaErr := s.discoverQueryDraftSchemaContext(ctx, target, prompt, selection)
	examples, exampleWarnings := s.queryDraftExamples(ctx, target, prompt, selection)
	disclosure := buildDataDisclosureReport(selection.IncludeSampleRows, schemaContext, len(examples) > 0)
	req := queryDraftRequest{Target: target, Prompt: prompt, SchemaContext: schemaContexts(schemaContext), Examples: examples, DataDisclosure: disclosure}
	agent, err := s.queryDraftAgentForAsk()
	if err != nil {
		return err
	}
	draft, err := agent.GenerateQueryDraft(ctx, req)
	if err != nil {
		return err
	}
	normalizeQueryDraft(&draft, req)
	if schemaErr != nil {
		draft.Warnings = append(draft.Warnings, "Schema Context discovery failed; Query Draft may be less accurate: "+schemaErr.Error())
	}
	draft.Warnings = append(draft.Warnings, exampleWarnings...)
	s.applyQueryPlanValidationAndRepair(ctx, &draft, req, agent, selection)
	s.applyExecutionGate(ctx, &draft, selection)
	return writeQueryDraft(s.output(), s.cfg.output, draft)
}

func parseAskArgs(args []string) (askTargetSelection, string, error) {
	var selection askTargetSelection
	promptArgs := []string{}
	for i := 0; i < len(args); {
		arg := args[i]
		switch {
		case arg == "--":
			promptArgs = append(promptArgs, args[i+1:]...)
			i = len(args)
		case arg == "--target" || arg == "--target-alias":
			if i+1 >= len(args) {
				return selection, "", fmt.Errorf("%s requires a value", arg)
			}
			selection.Alias = args[i+1]
			if strings.TrimSpace(selection.Alias) == "" {
				return selection, "", fmt.Errorf("%s requires a non-empty value", arg)
			}
			i += 2
		case strings.HasPrefix(arg, "--target="):
			selection.Alias = strings.TrimPrefix(arg, "--target=")
			if strings.TrimSpace(selection.Alias) == "" {
				return selection, "", errors.New("--target requires a non-empty value")
			}
			i++
		case strings.HasPrefix(arg, "--target-alias="):
			selection.Alias = strings.TrimPrefix(arg, "--target-alias=")
			if strings.TrimSpace(selection.Alias) == "" {
				return selection, "", errors.New("--target-alias requires a non-empty value")
			}
			i++
		case arg == "--service-uri" || arg == "--service-url":
			if i+1 >= len(args) {
				return selection, "", fmt.Errorf("%s requires a value", arg)
			}
			selection.ServiceURI = args[i+1]
			selection.ServiceURISet = true
			i += 2
		case strings.HasPrefix(arg, "--service-uri="):
			selection.ServiceURI = strings.TrimPrefix(arg, "--service-uri=")
			selection.ServiceURISet = true
			i++
		case strings.HasPrefix(arg, "--service-url="):
			selection.ServiceURI = strings.TrimPrefix(arg, "--service-url=")
			selection.ServiceURISet = true
			i++
		case arg == "--database":
			if i+1 >= len(args) {
				return selection, "", errors.New("--database requires a value")
			}
			selection.Database = args[i+1]
			selection.DatabaseSet = true
			i += 2
		case strings.HasPrefix(arg, "--database="):
			selection.Database = strings.TrimPrefix(arg, "--database=")
			selection.DatabaseSet = true
			i++
		case arg == "--execute":
			selection.Execute = true
			i++
		case strings.HasPrefix(arg, "--execute="):
			value, err := strconv.ParseBool(strings.TrimPrefix(arg, "--execute="))
			if err != nil {
				return selection, "", fmt.Errorf("--execute requires a boolean value: %w", err)
			}
			selection.Execute = value
			i++
		case arg == "--validate-plan":
			selection.ValidatePlan = true
			selection.ValidatePlanSet = true
			i++
		case strings.HasPrefix(arg, "--validate-plan="):
			value, err := strconv.ParseBool(strings.TrimPrefix(arg, "--validate-plan="))
			if err != nil {
				return selection, "", fmt.Errorf("--validate-plan requires a boolean value: %w", err)
			}
			selection.ValidatePlan = value
			selection.ValidatePlanSet = true
			i++
		case arg == "--repair":
			selection.Repair = true
			selection.RepairSet = true
			i++
		case strings.HasPrefix(arg, "--repair="):
			value, err := strconv.ParseBool(strings.TrimPrefix(arg, "--repair="))
			if err != nil {
				return selection, "", fmt.Errorf("--repair requires a boolean value: %w", err)
			}
			selection.Repair = value
			selection.RepairSet = true
			i++
		case arg == "--max-repair-attempts":
			if i+1 >= len(args) {
				return selection, "", errors.New("--max-repair-attempts requires a value")
			}
			maxAttempts, err := parseAskRepairMaxAttempts(args[i+1], arg)
			if err != nil {
				return selection, "", err
			}
			selection.MaxRepairAttempts = maxAttempts
			selection.MaxRepairSet = true
			i += 2
		case strings.HasPrefix(arg, "--max-repair-attempts="):
			maxAttempts, err := parseAskRepairMaxAttempts(strings.TrimPrefix(arg, "--max-repair-attempts="), "--max-repair-attempts")
			if err != nil {
				return selection, "", err
			}
			selection.MaxRepairAttempts = maxAttempts
			selection.MaxRepairSet = true
			i++
		case arg == "--max-rows" || arg == "--execute-max-rows":
			if i+1 >= len(args) {
				return selection, "", fmt.Errorf("%s requires a value", arg)
			}
			maxRecords, err := parseAskExecutionMaxRecords(args[i+1], arg)
			if err != nil {
				return selection, "", err
			}
			selection.MaxRecords = maxRecords
			i += 2
		case strings.HasPrefix(arg, "--max-rows="):
			maxRecords, err := parseAskExecutionMaxRecords(strings.TrimPrefix(arg, "--max-rows="), "--max-rows")
			if err != nil {
				return selection, "", err
			}
			selection.MaxRecords = maxRecords
			i++
		case strings.HasPrefix(arg, "--execute-max-rows="):
			maxRecords, err := parseAskExecutionMaxRecords(strings.TrimPrefix(arg, "--execute-max-rows="), "--execute-max-rows")
			if err != nil {
				return selection, "", err
			}
			selection.MaxRecords = maxRecords
			i++
		case arg == "--include-samples" || arg == "--include-sample-rows":
			selection.IncludeSampleRows = true
			i++
		case strings.HasPrefix(arg, "--include-samples="):
			value, err := strconv.ParseBool(strings.TrimPrefix(arg, "--include-samples="))
			if err != nil {
				return selection, "", fmt.Errorf("--include-samples requires a boolean value: %w", err)
			}
			selection.IncludeSampleRows = value
			i++
		case strings.HasPrefix(arg, "--include-sample-rows="):
			value, err := strconv.ParseBool(strings.TrimPrefix(arg, "--include-sample-rows="))
			if err != nil {
				return selection, "", fmt.Errorf("--include-sample-rows requires a boolean value: %w", err)
			}
			selection.IncludeSampleRows = value
			i++
		case arg == "--shots-table":
			if i+1 >= len(args) {
				return selection, "", errors.New("--shots-table requires a value")
			}
			selection.ShotsTableName = strings.TrimSpace(args[i+1])
			if selection.ShotsTableName == "" {
				return selection, "", errors.New("--shots-table requires a non-empty value")
			}
			selection.ShotsTableSet = true
			i += 2
		case strings.HasPrefix(arg, "--shots-table="):
			selection.ShotsTableName = strings.TrimSpace(strings.TrimPrefix(arg, "--shots-table="))
			if selection.ShotsTableName == "" {
				return selection, "", errors.New("--shots-table requires a non-empty value")
			}
			selection.ShotsTableSet = true
			i++
		case arg == "--shots-limit":
			if i+1 >= len(args) {
				return selection, "", errors.New("--shots-limit requires a value")
			}
			limit, err := parseAskShotsLimit(args[i+1], arg)
			if err != nil {
				return selection, "", err
			}
			selection.ShotsLimit = limit
			selection.ShotsLimitSet = true
			i += 2
		case strings.HasPrefix(arg, "--shots-limit="):
			limit, err := parseAskShotsLimit(strings.TrimPrefix(arg, "--shots-limit="), "--shots-limit")
			if err != nil {
				return selection, "", err
			}
			selection.ShotsLimit = limit
			selection.ShotsLimitSet = true
			i++
		default:
			promptArgs = append(promptArgs, args[i:]...)
			i = len(args)
		}
	}
	prompt := strings.TrimSpace(strings.Join(promptArgs, " "))
	if prompt == "" {
		return selection, "", errors.New("usage: kusto-cli ask '<natural-language prompt>'")
	}
	return selection, prompt, nil
}

func parseAskExecutionMaxRecords(value, flagName string) (int, error) {
	maxRecords, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s requires an integer value: %w", flagName, err)
	}
	if maxRecords <= 0 {
		return 0, fmt.Errorf("%s requires a value greater than zero", flagName)
	}
	return maxRecords, nil
}

func parseAskShotsLimit(value, flagName string) (int, error) {
	limit, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s requires an integer value: %w", flagName, err)
	}
	if limit <= 0 {
		return 0, fmt.Errorf("%s requires a value greater than zero", flagName)
	}
	if limit > maxQueryDraftShotsLimit {
		return 0, fmt.Errorf("%s must be %d or less", flagName, maxQueryDraftShotsLimit)
	}
	return limit, nil
}

func parseAskRepairMaxAttempts(value, flagName string) (int, error) {
	maxAttempts, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s requires an integer value: %w", flagName, err)
	}
	if maxAttempts <= 0 {
		return 0, fmt.Errorf("%s requires a value greater than zero", flagName)
	}
	if maxAttempts > maxAskRepairMaxAttempts {
		return 0, fmt.Errorf("%s must be %d or less", flagName, maxAskRepairMaxAttempts)
	}
	return maxAttempts, nil
}

func applyConfiguredAskDefaults(selection askTargetSelection) (askTargetSelection, error) {
	if !selection.ValidatePlanSet {
		value, ok, err := configuredBool("KUSTO_ASK_VALIDATE_PLAN")
		if err != nil {
			return selection, err
		}
		if ok {
			selection.ValidatePlan = value
		}
	}
	if !selection.RepairSet {
		value, ok, err := configuredBool("KUSTO_ASK_REPAIR")
		if err != nil {
			return selection, err
		}
		if ok {
			selection.Repair = value
		}
	}
	if !selection.MaxRepairSet {
		value := os.Getenv("KUSTO_ASK_MAX_REPAIR_ATTEMPTS")
		if strings.TrimSpace(value) != "" {
			maxAttempts, err := parseAskRepairMaxAttempts(value, "ask max repair attempts")
			if err != nil {
				return selection, err
			}
			selection.MaxRepairAttempts = maxAttempts
		}
	}
	return selection, nil
}

func configuredBool(envName string) (bool, bool, error) {
	if value := strings.TrimSpace(os.Getenv(envName)); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return false, false, fmt.Errorf("%s requires a boolean value: %w", envName, err)
		}
		return parsed, true, nil
	}
	return false, false, nil
}

func (s *server) queryDraftTarget(selection askTargetSelection) (queryDraftTarget, error) {
	database, databaseSet := s.askSelectedDatabase(selection)
	if alias := strings.TrimSpace(selection.Alias); alias != "" {
		return s.queryDraftTargetByAlias(alias, database, databaseSet)
	}
	if selection.ServiceURISet {
		return s.queryDraftTargetByServiceURI(selection.ServiceURI, database, databaseSet)
	}
	if strings.TrimSpace(s.cfg.targetAlias) != "" && s.cfg.targetAliasFlag {
		return s.queryDraftTargetByAlias(s.cfg.targetAlias, database, databaseSet)
	}
	if s.cfg.serviceURIFlag {
		return s.queryDraftTargetByServiceURI(s.cfg.serviceURI, database, databaseSet)
	}
	if clusterURI, serviceURISet := s.askSelectedServiceURI(selection); serviceURISet && databaseSet {
		return s.queryDraftTargetByServiceURI(clusterURI, database, databaseSet)
	}
	if alias := strings.TrimSpace(s.cfg.targetAlias); alias != "" {
		return s.queryDraftTargetByAlias(alias, database, databaseSet)
	}
	if clusterURI, serviceURISet := s.askSelectedServiceURI(selection); serviceURISet {
		return s.queryDraftTargetByServiceURI(clusterURI, database, databaseSet)
	}
	return s.queryDraftTargetFromCatalog(database, databaseSet)
}

func (s *server) askSelectedServiceURI(selection askTargetSelection) (string, bool) {
	if selection.ServiceURISet {
		return strings.TrimSpace(selection.ServiceURI), true
	}
	if strings.TrimSpace(s.cfg.serviceURI) != "" {
		return strings.TrimSpace(s.cfg.serviceURI), true
	}
	return "", false
}

func (s *server) askSelectedDatabase(selection askTargetSelection) (string, bool) {
	if selection.DatabaseSet {
		return strings.TrimSpace(selection.Database), true
	}
	if s.cfg.hasConfiguredDatabase() {
		return strings.TrimSpace(s.cfg.database), true
	}
	return "", false
}

func (s *server) queryDraftTargetByAlias(alias, database string, databaseSet bool) (queryDraftTarget, error) {
	var matches []queryDraftTarget
	aliasWasConfigured := false
	for _, svc := range s.knownServices {
		if !svc.matchesTargetAlias(alias) {
			continue
		}
		aliasWasConfigured = true
		db := strings.TrimSpace(svc.DefaultDatabase)
		if db == "" && databaseSet {
			db = strings.TrimSpace(database)
		}
		if db == "" {
			continue
		}
		matches = appendUniqueQueryDraftTarget(matches, queryDraftTarget{ClusterURI: svc.ServiceURI, Database: db})
	}
	switch len(matches) {
	case 1:
		return validateQueryDraftTarget(matches[0])
	case 0:
		if aliasWasConfigured {
			return queryDraftTarget{}, fmt.Errorf("target alias %q does not resolve a database; add default_database to its Target Catalog entry or provide --database", alias)
		}
		if len(s.allTargetAliases()) == 0 {
			return queryDraftTarget{}, fmt.Errorf("target alias %q was not found; no Target Catalog aliases are configured", alias)
		}
		return queryDraftTarget{}, fmt.Errorf("target alias %q was not found; available targets:\n%s", alias, formatAskTargets(s.askTargetCatalog()))
	default:
		return queryDraftTarget{}, fmt.Errorf("target alias %q is ambiguous; matching targets:\n%s", alias, formatQueryDraftTargets(matches))
	}
}

func (s *server) queryDraftTargetByServiceURI(clusterURI, database string, databaseSet bool) (queryDraftTarget, error) {
	clusterURI = strings.TrimSpace(clusterURI)
	if clusterURI == "" {
		return queryDraftTarget{}, errors.New("ask requires a Target service URI; provide --service-uri or select --target")
	}
	clusterURI = strings.TrimRight(clusterURI, "/")
	if _, err := url.ParseRequestURI(clusterURI); err != nil {
		return queryDraftTarget{}, fmt.Errorf("service-uri is not a valid URL: %w", err)
	}
	if databaseSet {
		if strings.TrimSpace(database) == "" {
			return queryDraftTarget{}, errors.New("ask requires a Target database; provide --database or select --target")
		}
		return validateQueryDraftTarget(queryDraftTarget{ClusterURI: clusterURI, Database: database})
	}
	matches := s.targetsForServiceURI(clusterURI)
	switch len(matches) {
	case 1:
		return validateQueryDraftTarget(matches[0])
	case 0:
		return queryDraftTarget{}, errors.New("ask requires a Target database; provide --database or select --target")
	default:
		return queryDraftTarget{}, fmt.Errorf("service-uri matches multiple configured targets; provide --database or select --target:\n%s", formatQueryDraftTargets(matches))
	}
}

func (s *server) queryDraftTargetFromCatalog(database string, databaseSet bool) (queryDraftTarget, error) {
	if databaseSet {
		if strings.TrimSpace(database) == "" {
			return queryDraftTarget{}, errors.New("ask requires a Target database; provide --database or select --target")
		}
		clusters := s.configuredServiceURIs()
		switch len(clusters) {
		case 1:
			return validateQueryDraftTarget(queryDraftTarget{ClusterURI: clusters[0], Database: database})
		case 0:
			return queryDraftTarget{}, errors.New("ask requires a Target service URI; provide --service-uri or select --target")
		default:
			return queryDraftTarget{}, fmt.Errorf("ask requires exactly one Target; multiple Target Catalog services are configured, select one with --target or --service-uri:\n%s", formatConfiguredServices(clusters))
		}
	}
	targets := s.askTargetCatalog()
	switch len(targets) {
	case 1:
		return validateQueryDraftTarget(queryDraftTarget{ClusterURI: targets[0].ClusterURI, Database: targets[0].Database})
	case 0:
		msg := "ask requires a Target; provide --service-uri and --database or select --target from the Target Catalog"
		if len(s.knownServices) > 0 {
			msg += "\nConfigured services without target databases:\n" + formatConfiguredServices(s.configuredServiceURIs())
		}
		return queryDraftTarget{}, errors.New(msg)
	default:
		return queryDraftTarget{}, fmt.Errorf("ask requires exactly one Target; multiple targets are configured, select one with --target:\n%s", formatAskTargets(targets))
	}
}

func (fakeQueryDraftAgent) GenerateQueryDraft(ctx context.Context, req queryDraftRequest) (queryDraft, error) {
	return fakeModelProvider{}.GenerateQueryDraft(ctx, req)
}

func normalizeQueryDraft(draft *queryDraft, req queryDraftRequest) {
	if draft.Format == "" {
		draft.Format = "query_draft"
	}
	if draft.Target.ClusterURI == "" {
		draft.Target.ClusterURI = req.Target.ClusterURI
	}
	if draft.Target.Database == "" {
		draft.Target.Database = req.Target.Database
	}
	if draft.Prompt == "" {
		draft.Prompt = req.Prompt
	}
	if draft.Assumptions == nil {
		draft.Assumptions = []string{}
	}
	if draft.Warnings == nil {
		draft.Warnings = []string{}
	}
	if draft.Examples == nil || (len(draft.Examples) == 0 && len(req.Examples) > 0) {
		draft.Examples = req.Examples
	}
	if draft.SchemaContext == nil || (len(draft.SchemaContext) == 0 && len(req.SchemaContext) > 0) {
		draft.SchemaContext = req.SchemaContext
	}
	if draft.SchemaContext == nil {
		draft.SchemaContext = []queryDraftSchemaContext{}
	}
	for i := range draft.SchemaContext {
		if draft.SchemaContext[i].Entities == nil {
			draft.SchemaContext[i].Entities = schemaContextEntities(draft.SchemaContext[i])
		}
		if draft.SchemaContext[i].Entities == nil {
			draft.SchemaContext[i].Entities = []string{}
		}
		if draft.SchemaContext[i].Warnings != nil && len(draft.SchemaContext[i].Warnings) == 0 {
			draft.SchemaContext[i].Warnings = nil
		}
	}
	if draft.DataDisclosure.Mode == "" {
		draft.DataDisclosure = req.DataDisclosure
	}
	if draft.DataDisclosure.Mode == "" {
		draft.DataDisclosure = buildDataDisclosureReport(false, queryDraftSchemaContext{}, false)
	}
	if req.DataDisclosure.Mode != "" {
		draft.DataDisclosure.Mode = req.DataDisclosure.Mode
		draft.DataDisclosure.Sent.Schema = req.DataDisclosure.Sent.Schema
		draft.DataDisclosure.Sent.Docstrings = req.DataDisclosure.Sent.Docstrings
		draft.DataDisclosure.Sent.Shots = req.DataDisclosure.Sent.Shots
		draft.DataDisclosure.Sent.SampleRows = req.DataDisclosure.Sent.SampleRows
	}
	draft.DataDisclosure.Sent.QueryResults = false
	draft.ClarificationQuestion = strings.TrimSpace(draft.ClarificationQuestion)
	if draft.ModelSafety != nil && (draft.ModelSafety.Classification != "" || draft.ModelSafety.Reason != "") {
		draft.ModelSafety.Advisory = true
	}
	draft.Validation = validateQueryDraftSafety(*draft)
	switch draft.Validation.Status {
	case "passed":
	case "clarification_required":
		draft.Warnings = append(draft.Warnings, "Clarification is required before executable KQL can be generated.")
	case "warning":
		draft.Warnings = append(draft.Warnings, "Generated query has validation warnings that block execution until resolved.")
	default:
		draft.Warnings = append(draft.Warnings, "Generated query failed safety validation; do not execute it until corrected.")
	}
	draft.Execution = queryDraftExecution{Executed: false, Reason: queryDraftExecutionReason(draft.Validation)}
}

func (s *server) applyExecutionGate(ctx context.Context, draft *queryDraft, selection askTargetSelection) {
	if draft == nil || !selection.Execute {
		return
	}
	maxRecords := askExecutionMaxRecords(selection)
	draft.Execution.MaxRecords = maxRecords
	if !draft.Validation.SafeForExecution {
		draft.Execution.Executed = false
		draft.Execution.Status = "blocked"
		draft.Execution.Reason = "blocked: Execution Gate requested, but Query Draft validation did not pass"
		if len(draft.Validation.Errors) > 0 {
			draft.Execution.Reason += ": " + strings.Join(draft.Validation.Errors, "; ")
		} else if len(draft.Validation.Warnings) > 0 {
			draft.Execution.Reason += ": " + strings.Join(draft.Validation.Warnings, "; ")
		}
		return
	}
	resp, err := s.executeKusto(ctx, draft.Target.ClusterURI, draft.Target.Database, draft.Query, "query", true, askExecutionClientRequestProperties(maxRecords))
	draft.Execution.Executed = true
	draft.Execution.MaxRecords = maxRecords
	if err != nil {
		draft.Execution.Status = "failed"
		draft.Execution.Reason = "execution failed after explicit Execution Gate"
		draft.Execution.Error = err.Error()
		draft.Warnings = append(draft.Warnings, "Execution Gate attempted query execution, but execution failed: "+err.Error())
		return
	}
	draft.Execution.Status = "succeeded"
	draft.Execution.Reason = "executed after explicit Execution Gate"
	draft.Execution.Result = &resp
}

func (s *server) applyQueryPlanValidationAndRepair(ctx context.Context, draft *queryDraft, req queryDraftRequest, agent queryDraftAgent, selection askTargetSelection) {
	if draft == nil {
		return
	}
	validatePlan := selection.ValidatePlan || selection.Repair
	maxAttempts := askRepairMaxAttempts(selection)
	attempts := 0
	for {
		trigger := ""
		validationError := ""
		validationErrors := []string{}

		if !draft.Validation.SafeForExecution {
			validationErrors = queryDraftValidationMessages(draft.Validation)
			validationError = strings.Join(validationErrors, "; ")
			if validationError == "" {
				validationError = "Query Draft safety validation did not pass"
			}
			trigger = "static_validation"
			if validatePlan && draft.Validation.QueryPlan == nil {
				draft.Validation.QueryPlan = &queryDraftPlanValidation{Requested: true, Status: "skipped", Message: "static Query Draft safety validation must pass before Kusto-side query-plan validation"}
			}
		} else if validatePlan {
			s.applyQueryPlanValidation(ctx, draft)
			if draft.Validation.QueryPlan == nil || draft.Validation.QueryPlan.Status == "passed" {
				return
			}
			if draft.Validation.QueryPlan.Status != "failed" {
				return
			}
			validationErrors = queryDraftValidationMessages(draft.Validation)
			validationError = strings.Join(validationErrors, "; ")
			if validationError == "" {
				validationError = draft.Validation.QueryPlan.Error
			}
			trigger = "query_plan_validation"
		} else {
			return
		}

		if !selection.Repair {
			return
		}
		if attempts >= maxAttempts {
			draft.Warnings = append(draft.Warnings, fmt.Sprintf("Repair Pass maximum reached (%d); returning last Query Draft without execution.", maxAttempts))
			draft.Execution = queryDraftExecution{Executed: false, Reason: queryDraftExecutionReason(draft.Validation)}
			return
		}
		repairer, ok := agent.(queryDraftRepairer)
		if !ok {
			draft.Warnings = append(draft.Warnings, "Repair Pass requested, but the configured Query Draft Agent does not support Repair Passes.")
			draft.Execution = queryDraftExecution{Executed: false, Reason: queryDraftExecutionReason(draft.Validation)}
			return
		}

		attempts++
		repairReq := queryDraftRepairRequest{
			Target:            req.Target,
			Prompt:            req.Prompt,
			SchemaContext:     req.SchemaContext,
			Examples:          req.Examples,
			DataDisclosure:    req.DataDisclosure,
			PreviousQuery:     strings.TrimSpace(draft.Query),
			ValidationError:   validationError,
			ValidationErrors:  validationErrors,
			RepairAttempt:     attempts,
			MaxRepairAttempts: maxAttempts,
		}
		repairReq.DataDisclosure.Sent.QueryResults = false
		history := queryDraftRepairPass{
			Attempt:         attempts,
			Trigger:         trigger,
			InputQuery:      strings.TrimSpace(draft.Query),
			ValidationError: validationError,
			Status:          "requested",
		}
		repaired, err := repairer.RepairQueryDraft(ctx, repairReq)
		if err != nil {
			history.Status = "failed"
			history.Error = err.Error()
			draft.RepairHistory = append(draft.RepairHistory, history)
			draft.Warnings = append(draft.Warnings, "Repair Pass failed: "+err.Error())
			draft.Execution = queryDraftExecution{Executed: false, Reason: queryDraftExecutionReason(draft.Validation)}
			return
		}
		history.OutputQuery = strings.TrimSpace(repaired.Query)
		history.Status = "generated"
		historySoFar := append([]queryDraftRepairPass{}, draft.RepairHistory...)
		historySoFar = append(historySoFar, history)
		normalizeQueryDraft(&repaired, req)
		repaired.RepairHistory = historySoFar
		*draft = repaired
	}
}

func (s *server) applyQueryPlanValidation(ctx context.Context, draft *queryDraft) {
	if draft == nil {
		return
	}
	if !draft.Validation.SafeForExecution {
		draft.Validation.QueryPlan = &queryDraftPlanValidation{Requested: true, Status: "skipped", Message: "static Query Draft safety validation must pass before Kusto-side query-plan validation"}
		return
	}
	plan := queryDraftPlanValidation{Requested: true}
	_, err := s.executeKusto(ctx, draft.Target.ClusterURI, draft.Target.Database, ".show queryplan <| "+strings.TrimSpace(draft.Query), "mgmt", true, nil)
	if err != nil {
		plan.Status = "failed"
		plan.Error = err.Error()
		message := "query-plan validation failed: " + err.Error()
		draft.Validation.QueryPlan = &plan
		draft.Validation.Status = "failed"
		draft.Validation.SafeForExecution = false
		draft.Validation.Checks = append(draft.Validation.Checks, queryDraftValidationCheck{Name: "query_plan_validation", Passed: false, Severity: "error", Message: message})
		draft.Validation.Errors = append(draft.Validation.Errors, message)
		draft.Execution = queryDraftExecution{Executed: false, Reason: queryDraftExecutionReason(draft.Validation)}
		return
	}
	plan.Status = "passed"
	plan.Message = "Kusto-side query-plan validation passed"
	draft.Validation.QueryPlan = &plan
	draft.Validation.Checks = append(draft.Validation.Checks, queryDraftValidationCheck{Name: "query_plan_validation", Passed: true})
}

func queryDraftValidationMessages(validation queryDraftValidation) []string {
	messages := append([]string{}, validation.Errors...)
	messages = append(messages, validation.Warnings...)
	if len(messages) == 0 && validation.QueryPlan != nil && validation.QueryPlan.Status == "failed" && validation.QueryPlan.Error != "" {
		messages = append(messages, validation.QueryPlan.Error)
	}
	return messages
}

func askRepairMaxAttempts(selection askTargetSelection) int {
	if selection.MaxRepairAttempts > 0 {
		return selection.MaxRepairAttempts
	}
	return defaultAskRepairMaxAttempts
}

func askExecutionMaxRecords(selection askTargetSelection) int {
	if selection.MaxRecords > 0 {
		return selection.MaxRecords
	}
	return defaultAskExecutionMaxRecords
}

func askExecutionClientRequestProperties(maxRecords int) map[string]any {
	if maxRecords <= 0 {
		maxRecords = defaultAskExecutionMaxRecords
	}
	return map[string]any{"query_take_max_records": maxRecords}
}

func (s *server) executeKusto(ctx context.Context, clusterURI, database, csl, kind string, readonly bool, crp map[string]any) (kustoResponse, error) {
	if s.executeHook != nil {
		return s.executeHook(ctx, clusterURI, database, csl, kind, readonly, crp)
	}
	return s.execute(ctx, clusterURI, database, csl, kind, readonly, crp)
}

func (s *server) discoverQueryDraftSchemaContext(ctx context.Context, target queryDraftTarget, prompt string, selection askTargetSelection) (queryDraftSchemaContext, error) {
	discoverer := s.schemaDiscoverer
	if discoverer == nil {
		discoverer = emptySchemaDiscoverer{}
	}
	schemaContext, err := discoverer.DiscoverSchemaContext(ctx, schemaDiscoveryRequest{
		Target:            target,
		Prompt:            prompt,
		IncludeSampleRows: selection.IncludeSampleRows,
		SampleRowLimit:    defaultSchemaContextSampleRows,
	})
	schemaContext = applyDataDisclosurePolicy(schemaContext, selection.IncludeSampleRows)
	if schemaContext.Entities == nil {
		schemaContext.Entities = schemaContextEntities(schemaContext)
	}
	return schemaContext, err
}

func (emptySchemaDiscoverer) DiscoverSchemaContext(context.Context, schemaDiscoveryRequest) (queryDraftSchemaContext, error) {
	return queryDraftSchemaContext{}, nil
}

func (s *server) queryDraftExamples(ctx context.Context, target queryDraftTarget, prompt string, selection askTargetSelection) ([]queryDraftExample, []string) {
	bundled := bundledQueryDraftExamples(prompt)
	configured, warnings, err := s.configuredQueryDraftShots(ctx, target, prompt, selection)
	if err != nil {
		warnings = append(warnings, "Configured shots retrieval failed; continuing with bundled examples only: "+err.Error())
	}
	examples := make([]queryDraftExample, 0, len(configured)+len(bundled))
	examples = append(examples, configured...)
	examples = append(examples, bundled...)
	return examples, warnings
}

func bundledQueryDraftExamples(prompt string) []queryDraftExample {
	all := []queryDraftExample{
		{
			Source: "bundled",
			Name:   "recent_rows",
			Intent: "Return recent rows with an explicit result bound.",
			Query:  "StormEvents\n| where StartTime > ago(1d)\n| project StartTime, State, EventType\n| take 100",
		},
		{
			Source: "bundled",
			Name:   "filter_project_take",
			Intent: "Filter rows, select relevant columns, and cap returned records.",
			Query:  "StormEvents\n| where State == 'WA'\n| project StartTime, State, EventType\n| take 100",
		},
		{
			Source: "bundled",
			Name:   "summarize_count_by_dimension",
			Intent: "Count rows by a categorical column and return the most common values.",
			Query:  "StormEvents\n| summarize EventCount = count() by State\n| top 10 by EventCount desc",
		},
		{
			Source: "bundled",
			Name:   "time_series_count",
			Intent: "Summarize rows into time buckets for a trend.",
			Query:  "StormEvents\n| summarize EventCount = count() by bin(StartTime, 1d)\n| order by StartTime desc\n| take 30",
		},
		{
			Source: "bundled",
			Name:   "distinct_values",
			Intent: "List distinct values from a column with an explicit cap.",
			Query:  "StormEvents\n| summarize by EventType\n| take 100",
		},
	}
	tokens := promptTokens(prompt)
	type scoredExample struct {
		example queryDraftExample
		score   int
		idx     int
	}
	scored := make([]scoredExample, 0, len(all))
	for i, example := range all {
		scored = append(scored, scoredExample{example: example, score: scoreText(example.Name+" "+example.Intent+" "+example.Query, tokens), idx: i})
	}
	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].idx < scored[j].idx
	})
	limit := min(maxBundledQueryDraftExamples, len(scored))
	examples := make([]queryDraftExample, 0, limit)
	for i := 0; i < limit; i++ {
		examples = append(examples, scored[i].example)
	}
	return examples
}

func (s *server) configuredQueryDraftShots(ctx context.Context, target queryDraftTarget, prompt string, selection askTargetSelection) ([]queryDraftExample, []string, error) {
	tableName := strings.TrimSpace(selection.ShotsTableName)
	if tableName == "" && !selection.ShotsTableSet {
		tableName = strings.TrimSpace(os.Getenv("KUSTO_SHOTS_TABLE"))
	}
	if tableName == "" {
		return nil, nil, nil
	}
	limit := selection.ShotsLimit
	if limit <= 0 {
		limit = defaultQueryDraftShotsLimit
	}
	query := queryDraftShotsQuery(tableName, prompt, limit)
	resp, err := s.executeKusto(ctx, target.ClusterURI, target.Database, query, "query", true, nil)
	if err != nil {
		return nil, nil, err
	}
	examples, warnings := queryDraftExamplesFromShotsResponse(resp)
	return examples, warnings, nil
}

func queryDraftShotsQuery(tableName, prompt string, limit int) string {
	if limit <= 0 {
		limit = defaultQueryDraftShotsLimit
	}
	return fmt.Sprintf("%s | where * has %s | take %d", kqlEscapeEntityName(tableName), kqlStringLiteral(prompt), limit)
}

func queryDraftExamplesFromShotsResponse(resp kustoResponse) ([]queryDraftExample, []string) {
	rows := rowsToDicts(resp)
	examples := make([]queryDraftExample, 0, len(rows))
	warnings := []string{}
	for i, row := range rows {
		example, ok := queryDraftExampleFromShotRow(row)
		if !ok {
			warnings = append(warnings, fmt.Sprintf("Configured shot row %d did not include a KQL query; skipped.", i+1))
			continue
		}
		if err := validateQueryDraftExample(example.Query); err != nil {
			warnings = append(warnings, fmt.Sprintf("Configured shot %q is not a safe read-only Query Draft example; skipped: %v", firstNonEmpty(example.Name, example.Intent, fmt.Sprintf("row %d", i+1)), err))
			continue
		}
		examples = append(examples, example)
	}
	return examples, warnings
}

func queryDraftExampleFromShotRow(row map[string]any) (queryDraftExample, bool) {
	query := firstRowString(row, "query", "kql", "csl", "kusto_query", "query_text", "example_query")
	query = strings.TrimSpace(query)
	if query == "" {
		return queryDraftExample{}, false
	}
	intent := firstRowString(row, "intent", "prompt", "question", "description", "request", "natural_language")
	name := firstRowString(row, "name", "title", "id", "example", "example_name")
	intent = strings.TrimSpace(intent)
	name = strings.TrimSpace(name)
	if intent == "" {
		intent = firstNonEmpty(name, "Configured Query Draft shot")
	}
	return queryDraftExample{Source: "configured", Name: name, Intent: intent, Query: query}, true
}

func firstRowString(row map[string]any, names ...string) string {
	for _, name := range names {
		for key, value := range row {
			if !strings.EqualFold(key, name) || value == nil {
				continue
			}
			switch v := value.(type) {
			case string:
				return v
			case json.Number:
				return v.String()
			default:
				return fmt.Sprint(v)
			}
		}
	}
	return ""
}

func validateQueryDraftExample(query string) error {
	validation := validateQueryDraftSafety(queryDraft{Query: query})
	if validation.SafeForExecution {
		return nil
	}
	messages := queryDraftValidationMessages(validation)
	if len(messages) == 0 {
		return errors.New("validation did not pass")
	}
	return errors.New(strings.Join(messages, "; "))
}

func schemaContexts(schemaContext queryDraftSchemaContext) []queryDraftSchemaContext {
	if schemaContextEmpty(schemaContext) {
		return []queryDraftSchemaContext{}
	}
	return []queryDraftSchemaContext{schemaContext}
}

func schemaContextEmpty(schemaContext queryDraftSchemaContext) bool {
	return strings.TrimSpace(schemaContext.Source) == "" && len(schemaContext.Entities) == 0 && len(schemaContext.Tables) == 0 && len(schemaContext.Functions) == 0 && len(schemaContext.Warnings) == 0
}

func schemaContextEntities(schemaContext queryDraftSchemaContext) []string {
	entities := []string{}
	for _, table := range schemaContext.Tables {
		entities = appendUniqueString(entities, table.Name)
	}
	for _, fn := range schemaContext.Functions {
		entities = appendUniqueString(entities, fn.Name)
	}
	return entities
}

func applyDataDisclosurePolicy(schemaContext queryDraftSchemaContext, includeSampleRows bool) queryDraftSchemaContext {
	if includeSampleRows {
		return schemaContext
	}
	for i := range schemaContext.Tables {
		schemaContext.Tables[i].SampleRows = nil
	}
	return schemaContext
}

func buildDataDisclosureReport(includeSampleRows bool, schemaContext queryDraftSchemaContext, includeShots bool) queryDraftDataDisclosure {
	mode := dataDisclosureModeSchemaOnly
	if includeSampleRows {
		mode = dataDisclosureModeSchemaAndSamples
	}
	return queryDraftDataDisclosure{
		Mode: mode,
		Sent: queryDraftDataDisclosureSent{
			Schema:       schemaContextHasSchema(schemaContext),
			Docstrings:   schemaContextHasDocstrings(schemaContext),
			Shots:        includeShots,
			SampleRows:   schemaContextHasSampleRows(schemaContext),
			QueryResults: false,
		},
	}
}

func schemaContextHasSchema(schemaContext queryDraftSchemaContext) bool {
	if len(schemaContext.Entities) > 0 || len(schemaContext.Tables) > 0 || len(schemaContext.Functions) > 0 {
		return true
	}
	for _, table := range schemaContext.Tables {
		if len(table.Columns) > 0 {
			return true
		}
	}
	return false
}

func schemaContextHasDocstrings(schemaContext queryDraftSchemaContext) bool {
	for _, table := range schemaContext.Tables {
		if strings.TrimSpace(table.DocString) != "" {
			return true
		}
		for _, column := range table.Columns {
			if strings.TrimSpace(column.DocString) != "" {
				return true
			}
		}
	}
	for _, fn := range schemaContext.Functions {
		if strings.TrimSpace(fn.DocString) != "" {
			return true
		}
	}
	return false
}

func schemaContextHasSampleRows(schemaContext queryDraftSchemaContext) bool {
	for _, table := range schemaContext.Tables {
		if len(table.SampleRows) > 0 {
			return true
		}
	}
	return false
}

func (d kustoSchemaDiscoverer) DiscoverSchemaContext(ctx context.Context, req schemaDiscoveryRequest) (queryDraftSchemaContext, error) {
	if d.server == nil {
		return queryDraftSchemaContext{}, nil
	}
	text, err := d.server.callTool(ctx, "kusto_describe_database", mustMarshal(map[string]any{
		"cluster_uri": req.Target.ClusterURI,
		"database":    req.Target.Database,
	}))
	if err != nil {
		return queryDraftSchemaContext{}, err
	}
	var resp kustoResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		return queryDraftSchemaContext{}, fmt.Errorf("parse schema discovery response: %w", err)
	}
	catalog := schemaCatalogFromKustoResponse(resp)
	schemaContext := compactSchemaCatalog(catalog, req.Prompt)
	if schemaContext.Source == "" {
		schemaContext.Source = "target-schema"
	}
	if !req.IncludeSampleRows || len(schemaContext.Tables) == 0 {
		return schemaContext, nil
	}
	limit := req.SampleRowLimit
	if limit <= 0 {
		limit = defaultSchemaContextSampleRows
	}
	for i := range schemaContext.Tables {
		rows, sampleErr := d.sampleTableRows(ctx, req.Target, schemaContext.Tables[i], limit)
		if sampleErr != nil {
			schemaContext.Warnings = append(schemaContext.Warnings, fmt.Sprintf("sample rows unavailable for %s: %v", schemaContext.Tables[i].Name, sampleErr))
			continue
		}
		schemaContext.Tables[i].SampleRows = rows
	}
	return schemaContext, nil
}

func (d kustoSchemaDiscoverer) sampleTableRows(ctx context.Context, target queryDraftTarget, table queryDraftSchemaTable, limit int) ([]map[string]any, error) {
	entityType := table.EntityType
	if entityType == "" {
		entityType = "table"
	}
	text, err := d.server.callTool(ctx, "kusto_sample_entity", mustMarshal(map[string]any{
		"cluster_uri": target.ClusterURI,
		"database":    target.Database,
		"entity_type": entityType,
		"entity_name": table.Name,
		"sample_size": limit,
	}))
	if err != nil {
		return nil, err
	}
	var resp kustoResponse
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		return nil, fmt.Errorf("parse sample rows response: %w", err)
	}
	rows := rowsToDicts(resp)
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

type schemaCatalog struct {
	Tables    []queryDraftSchemaTable
	Functions []queryDraftSchemaFunction
}

func schemaCatalogFromKustoResponse(resp kustoResponse) schemaCatalog {
	columnIndex := map[string]int{}
	for i, column := range resp.Data.Columns {
		columnIndex[strings.ToLower(column.ColumnName)] = i
	}
	value := func(row []any, names ...string) string {
		for _, name := range names {
			idx, ok := columnIndex[strings.ToLower(name)]
			if !ok || idx >= len(row) || row[idx] == nil {
				continue
			}
			return strings.TrimSpace(fmt.Sprint(row[idx]))
		}
		return ""
	}
	catalog := schemaCatalog{}
	for _, row := range resp.Data.Rows {
		name := value(row, "EntityName", "Name", "TableName", "FunctionName")
		if name == "" {
			continue
		}
		entityType := normalizeSchemaEntityType(value(row, "EntityType", "Kind", "Type"))
		docString := value(row, "DocString", "Docstring", "Description")
		inputSchema := value(row, "CslInputSchema", "InputSchema", "Schema")
		outputSchema := value(row, "CslOutputSchema", "OutputSchema")
		switch entityType {
		case "table", "external-table", "materialized-view":
			columns := parseCSLColumns(firstNonEmpty(outputSchema, inputSchema, value(row, "Content")))
			catalog.Tables = append(catalog.Tables, queryDraftSchemaTable{EntityType: entityType, Name: name, DocString: docString, Columns: columns})
		case "function":
			catalog.Functions = append(catalog.Functions, queryDraftSchemaFunction{
				Name:          name,
				DocString:     docString,
				InputSchema:   inputSchema,
				OutputColumns: parseCSLColumns(outputSchema),
			})
		}
	}
	return catalog
}

func normalizeSchemaEntityType(entityType string) string {
	compact := strings.ToLower(strings.TrimSpace(entityType))
	compact = strings.ReplaceAll(compact, "_", "")
	compact = strings.ReplaceAll(compact, "-", "")
	compact = strings.ReplaceAll(compact, " ", "")
	switch compact {
	case "table", "tables":
		return "table"
	case "externaltable", "externaltables":
		return "external-table"
	case "materializedview", "materializedviews", "mv", "mvs":
		return "materialized-view"
	case "function", "functions", "storedfunction", "storedfunctions":
		return "function"
	default:
		return compact
	}
}

func compactSchemaCatalog(catalog schemaCatalog, prompt string) queryDraftSchemaContext {
	tokens := promptTokens(prompt)
	tables, tablesTruncated := compactTables(catalog.Tables, tokens)
	functions, functionsTruncated := compactFunctions(catalog.Functions, tokens)
	schemaContext := queryDraftSchemaContext{
		Source:    "target-schema",
		Tables:    tables,
		Functions: functions,
		Truncated: tablesTruncated || functionsTruncated,
	}
	schemaContext.Entities = schemaContextEntities(schemaContext)
	return schemaContext
}

func compactTables(tables []queryDraftSchemaTable, tokens []string) ([]queryDraftSchemaTable, bool) {
	if len(tables) == 0 {
		return nil, false
	}
	type scoredTable struct {
		table queryDraftSchemaTable
		score int
		idx   int
	}
	scored := make([]scoredTable, 0, len(tables))
	for i, table := range tables {
		score := scoreTable(table, tokens)
		table.Columns = compactColumns(table.Columns, tokens)
		scored = append(scored, scoredTable{table: table, score: score, idx: i})
	}
	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return strings.ToLower(scored[i].table.Name) < strings.ToLower(scored[j].table.Name)
	})
	hasPositiveScore := false
	for _, candidate := range scored {
		if candidate.score > 0 {
			hasPositiveScore = true
			break
		}
	}
	selected := make([]queryDraftSchemaTable, 0, min(len(scored), maxSchemaContextTables))
	for _, candidate := range scored {
		if len(selected) >= maxSchemaContextTables {
			break
		}
		if len(tokens) > 0 && hasPositiveScore && candidate.score == 0 {
			continue
		}
		selected = append(selected, candidate.table)
	}
	if len(selected) == 0 && len(scored) > 0 {
		limit := min(len(scored), maxSchemaContextTables)
		for i := 0; i < limit; i++ {
			selected = append(selected, scored[i].table)
		}
	}
	truncated := len(selected) < len(tables)
	for _, table := range selected {
		if len(table.Columns) == maxSchemaContextColumns {
			for _, original := range tables {
				if original.Name == table.Name && len(original.Columns) > maxSchemaContextColumns {
					truncated = true
					break
				}
			}
		}
	}
	return selected, truncated
}

func compactFunctions(functions []queryDraftSchemaFunction, tokens []string) ([]queryDraftSchemaFunction, bool) {
	if len(functions) == 0 {
		return nil, false
	}
	type scoredFunction struct {
		function queryDraftSchemaFunction
		score    int
	}
	scored := make([]scoredFunction, 0, len(functions))
	for _, fn := range functions {
		scored = append(scored, scoredFunction{function: fn, score: scoreFunction(fn, tokens)})
	}
	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return strings.ToLower(scored[i].function.Name) < strings.ToLower(scored[j].function.Name)
	})
	selected := []queryDraftSchemaFunction{}
	for _, candidate := range scored {
		if len(selected) >= maxSchemaContextFunctions {
			break
		}
		if len(tokens) > 0 && candidate.score == 0 {
			continue
		}
		selected = append(selected, candidate.function)
	}
	return selected, len(selected) < len(functions)
}

func compactColumns(columns []queryDraftSchemaColumn, tokens []string) []queryDraftSchemaColumn {
	if len(columns) <= maxSchemaContextColumns {
		if columns == nil {
			return []queryDraftSchemaColumn{}
		}
		return columns
	}
	type scoredColumn struct {
		column queryDraftSchemaColumn
		score  int
		idx    int
	}
	scored := make([]scoredColumn, 0, len(columns))
	for i, column := range columns {
		scored = append(scored, scoredColumn{column: column, score: scoreText(column.Name+" "+column.DocString, tokens), idx: i})
	}
	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].idx < scored[j].idx
	})
	selected := scored[:maxSchemaContextColumns]
	sort.SliceStable(selected, func(i, j int) bool { return selected[i].idx < selected[j].idx })
	out := make([]queryDraftSchemaColumn, 0, len(selected))
	for _, candidate := range selected {
		out = append(out, candidate.column)
	}
	return out
}

func scoreTable(table queryDraftSchemaTable, tokens []string) int {
	score := scoreText(table.Name, tokens) * 5
	score += scoreText(table.DocString, tokens) * 2
	for _, column := range table.Columns {
		score += scoreText(column.Name, tokens)
		score += scoreText(column.DocString, tokens)
	}
	return score
}

func scoreFunction(fn queryDraftSchemaFunction, tokens []string) int {
	score := scoreText(fn.Name, tokens) * 5
	score += scoreText(fn.DocString, tokens) * 2
	score += scoreText(fn.InputSchema, tokens)
	for _, column := range fn.OutputColumns {
		score += scoreText(column.Name, tokens)
	}
	return score
}

func scoreText(text string, tokens []string) int {
	if len(tokens) == 0 || strings.TrimSpace(text) == "" {
		return 0
	}
	lower := strings.ToLower(text)
	score := 0
	for _, token := range tokens {
		if strings.Contains(lower, token) {
			score++
		}
	}
	return score
}

func promptTokens(prompt string) []string {
	seen := map[string]bool{}
	tokens := []string{}
	for _, field := range strings.FieldsFunc(strings.ToLower(prompt), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	}) {
		if len(field) < 2 || seen[field] {
			continue
		}
		seen[field] = true
		tokens = append(tokens, field)
	}
	return tokens
}

func parseCSLColumns(schema string) []queryDraftSchemaColumn {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return []queryDraftSchemaColumn{}
	}
	if start := strings.Index(schema, "("); start >= 0 {
		if end := strings.LastIndex(schema, ")"); end > start {
			schema = schema[start+1 : end]
		}
	}
	parts := splitTopLevel(schema, ',')
	columns := []queryDraftSchemaColumn{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		colon := indexTopLevelColon(part)
		if colon <= 0 {
			continue
		}
		name := unquoteKQLIdentifier(part[:colon])
		columnType := strings.TrimSpace(part[colon+1:])
		if name == "" || columnType == "" {
			continue
		}
		columns = append(columns, queryDraftSchemaColumn{Name: name, Type: columnType})
	}
	return columns
}

func splitTopLevel(s string, sep rune) []string {
	parts := []string{}
	start := 0
	bracketDepth := 0
	parenDepth := 0
	inSingleQuote := false
	for i, r := range s {
		switch r {
		case '\'':
			inSingleQuote = !inSingleQuote
		case '[':
			if !inSingleQuote {
				bracketDepth++
			}
		case ']':
			if !inSingleQuote && bracketDepth > 0 {
				bracketDepth--
			}
		case '(':
			if !inSingleQuote {
				parenDepth++
			}
		case ')':
			if !inSingleQuote && parenDepth > 0 {
				parenDepth--
			}
		default:
			if r == sep && !inSingleQuote && bracketDepth == 0 && parenDepth == 0 {
				parts = append(parts, s[start:i])
				start = i + len(string(r))
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func indexTopLevelColon(s string) int {
	bracketDepth := 0
	inSingleQuote := false
	for i, r := range s {
		switch r {
		case '\'':
			inSingleQuote = !inSingleQuote
		case '[':
			if !inSingleQuote {
				bracketDepth++
			}
		case ']':
			if !inSingleQuote && bracketDepth > 0 {
				bracketDepth--
			}
		case ':':
			if !inSingleQuote && bracketDepth == 0 {
				return i
			}
		}
	}
	return -1
}

func unquoteKQLIdentifier(name string) string {
	name = strings.TrimSpace(name)
	if strings.HasPrefix(name, "['") && strings.HasSuffix(name, "']") {
		return strings.ReplaceAll(name[2:len(name)-2], "''", "'")
	}
	if strings.HasPrefix(name, "[") && strings.HasSuffix(name, "]") {
		name = strings.TrimSpace(name[1 : len(name)-1])
	}
	name = strings.Trim(name, "`\"'")
	return strings.TrimSpace(name)
}

var (
	queryDraftResultBoundPattern  = regexp.MustCompile(`(?i)(^|\|)\s*(take|limit|top|sample)\s+\d+\b`)
	queryDraftReducingPattern     = regexp.MustCompile(`(?i)(^|\|)\s*(count|summarize|make-series)\b`)
	queryDraftLeadingProsePattern = regexp.MustCompile(`(?i)^\s*(here\s+is\b|here's\b|sure\b[,.]?|the\s+query\s+is\b|you\s+can\s+use\b|kql\s*:)`)
	queryDraftWritePatterns       = []struct {
		name string
		re   *regexp.Regexp
	}{
		{name: "ingest", re: regexp.MustCompile(`(?i)\bingest(?:ion)?\b`)},
		{name: "into_table", re: regexp.MustCompile(`(?i)\binto\s+(table|database|cluster)\b`)},
		{name: "set_or_append_replace", re: regexp.MustCompile(`(?i)\bset\s*-\s*or\s*-\s*(append|replace)\b`)},
		{name: "delete", re: regexp.MustCompile(`(?i)\bdelete\s+(from|table|database|records?)\b`)},
		{name: "drop", re: regexp.MustCompile(`(?i)\bdrop\s+(table|database|column|function|materialized\s+view|external\s+table)\b`)},
		{name: "purge", re: regexp.MustCompile(`(?i)\bpurge\s+(table|database|records?|data)\b`)},
		{name: "alter", re: regexp.MustCompile(`(?i)\balter\s+(table|database|column|function|materialized\s+view|external\s+table)\b`)},
		{name: "create", re: regexp.MustCompile(`(?i)\bcreate(?:\s*-\s*or\s*-\s*alter|\s*-\s*merge)?\s+(table|database|function|materialized\s+view|external\s+table)\b`)},
		{name: "rename", re: regexp.MustCompile(`(?i)\brename\s+(table|database|column|function)\b`)},
		{name: "move_extents", re: regexp.MustCompile(`(?i)\bmove\s+extents?\b`)},
		{name: "truncate", re: regexp.MustCompile(`(?i)\btruncate\s+(table|database)\b`)},
	}
)

type queryDraftKQLAnalysis struct {
	statements              []string
	managementCommand       bool
	writeCapableDestructive bool
	safeStatementShape      bool
	statementShapeMessage   string
	bounded                 bool
	resultBoundMessage      string
	rawKQLOnly              bool
	rawKQLMessage           string
}

func validateQueryDraftShape(query string) queryDraftValidation {
	return validateQueryDraftSafety(queryDraft{Query: query})
}

func validateQueryDraftSafety(draft queryDraft) queryDraftValidation {
	if draft.ClarificationRequired {
		message := "clarification required before generating executable KQL"
		if draft.ClarificationQuestion != "" {
			message += ": " + draft.ClarificationQuestion
		}
		check := queryDraftValidationCheck{Name: "clarification_required", Passed: false, Severity: "error", Message: message}
		return queryDraftValidation{
			Status:           "clarification_required",
			ReadOnly:         false,
			Bounded:          false,
			SafeForExecution: false,
			Checks:           []queryDraftValidationCheck{check},
			Warnings:         []string{},
			Errors:           []string{message},
		}
	}

	query := strings.TrimSpace(draft.Query)
	analysis := analyzeQueryDraftKQL(query)
	checks := []queryDraftValidationCheck{}
	errs := []string{}
	warnings := []string{}
	addCheck := func(name string, passed bool, severity, message string) {
		check := queryDraftValidationCheck{Name: name, Passed: passed}
		if !passed {
			check.Severity = severity
			check.Message = message
			switch severity {
			case "warning":
				warnings = append(warnings, message)
			default:
				errs = append(errs, message)
			}
		}
		checks = append(checks, check)
	}

	addCheck("query_not_empty", query != "", "error", "query is required")
	addCheck("raw_kql_only", analysis.rawKQLOnly, "error", analysis.rawKQLMessage)
	addCheck("no_management_commands", !analysis.managementCommand, "error", "generated query must not be a management command")
	addCheck("no_write_capable_or_destructive_output", !analysis.writeCapableDestructive, "error", "generated query contains obvious write-capable or destructive KQL shape")
	addCheck("safe_statement_shape", analysis.safeStatementShape, "error", analysis.statementShapeMessage)
	addCheck("bounded_result", analysis.bounded, "warning", analysis.resultBoundMessage)

	readOnly := query != "" && analysis.rawKQLOnly && !analysis.managementCommand && !analysis.writeCapableDestructive && analysis.safeStatementShape
	status := "passed"
	if len(errs) > 0 {
		status = "failed"
	} else if len(warnings) > 0 {
		status = "warning"
	}
	return queryDraftValidation{
		Status:           status,
		ReadOnly:         readOnly,
		Bounded:          analysis.bounded,
		SafeForExecution: status == "passed" && readOnly && analysis.bounded,
		Checks:           checks,
		Warnings:         warnings,
		Errors:           errs,
	}
}

func analyzeQueryDraftKQL(query string) queryDraftKQLAnalysis {
	trimmed := strings.TrimSpace(query)
	rawKQLOnly, rawKQLMessage := validateRawQueryDraftKQL(trimmed)
	analysis := queryDraftKQLAnalysis{
		rawKQLOnly:            rawKQLOnly,
		rawKQLMessage:         rawKQLMessage,
		safeStatementShape:    true,
		resultBoundMessage:    "generated query must include an explicit result bound (take, limit, top, or sample) or a reducing aggregation before it can be executed",
		statementShapeMessage: "generated query must contain exactly one executable query statement; only let and declare query_parameters statements may precede it",
	}
	if trimmed == "" {
		analysis.statements = []string{}
		analysis.safeStatementShape = false
		analysis.statementShapeMessage = "generated query must contain one read-only KQL query statement"
		return analysis
	}

	statements := splitKQLStatements(trimmed)
	analysis.statements = statements
	analysis.managementCommand = hasManagementCommandStatement(trimmed, statements)
	analysis.writeCapableDestructive = hasWriteCapableOrDestructiveShape(trimmed)
	analysis.safeStatementShape, analysis.statementShapeMessage = validateQueryDraftStatementShape(statements)
	analysis.bounded = isBoundedQueryDraft(statements)
	return analysis
}

func validateRawQueryDraftKQL(query string) (bool, string) {
	if strings.Contains(query, "```") {
		return false, "generated query must be raw KQL, not a markdown code fence"
	}
	if queryDraftLeadingProsePattern.MatchString(query) {
		return false, "generated query must be raw KQL, not prose"
	}
	return true, ""
}

func queryDraftExecutionReason(validation queryDraftValidation) string {
	switch validation.Status {
	case "passed":
		return "generate-only; execution requires an explicit execution gate"
	case "clarification_required":
		return "blocked: clarification required before executable KQL can be generated"
	case "warning":
		return "blocked: Query Draft validation warnings must be resolved before execution"
	default:
		return "blocked: Query Draft validation failed; generated KQL is not safe to execute"
	}
}

func hasManagementCommandStatement(query string, statements []string) bool {
	for _, line := range strings.Split(query, "\n") {
		s := strings.TrimSpace(line)
		if s == "" || strings.HasPrefix(s, "//") || strings.HasPrefix(s, "#") {
			continue
		}
		if strings.HasPrefix(s, ".") {
			return true
		}
	}
	for _, stmt := range statements {
		if strings.HasPrefix(firstKQLContentLine(stmt), ".") {
			return true
		}
	}
	return false
}

func hasWriteCapableOrDestructiveShape(query string) bool {
	sanitized := stripKQLCommentsAndStrings(query)
	for _, pattern := range queryDraftWritePatterns {
		if pattern.re.MatchString(sanitized) {
			return true
		}
	}
	return false
}

func validateQueryDraftStatementShape(statements []string) (bool, string) {
	if len(statements) == 0 {
		return false, "generated query must contain one read-only KQL query statement"
	}
	executableStatements := 0
	for i, stmt := range statements {
		head := strings.ToLower(firstKQLContentLine(stripKQLCommentsAndStrings(stmt)))
		if head == "" {
			continue
		}
		if strings.HasPrefix(head, "set ") {
			return false, "generated query must not include set statements; request options are controlled by the CLI"
		}
		if isAllowedQueryDraftDeclaration(head) {
			if i == len(statements)-1 {
				return false, "generated query must end with a read-only query expression, not only declarations"
			}
			continue
		}
		executableStatements++
		if i != len(statements)-1 {
			return false, "multiple executable KQL statements are not allowed; use let declarations followed by one query"
		}
	}
	if executableStatements != 1 {
		return false, "generated query must contain exactly one executable query statement"
	}
	return true, ""
}

func isAllowedQueryDraftDeclaration(head string) bool {
	head = strings.TrimSpace(strings.ToLower(head))
	return strings.HasPrefix(head, "let ") || strings.HasPrefix(head, "declare query_parameters")
}

func isBoundedQueryDraft(statements []string) bool {
	stmt := finalExecutableQueryStatement(statements)
	if strings.TrimSpace(stmt) == "" {
		return false
	}
	sanitized := stripKQLCommentsAndStrings(stmt)
	return queryDraftResultBoundPattern.MatchString(sanitized) || queryDraftReducingPattern.MatchString(sanitized)
}

func finalExecutableQueryStatement(statements []string) string {
	for i := len(statements) - 1; i >= 0; i-- {
		head := strings.ToLower(firstKQLContentLine(stripKQLCommentsAndStrings(statements[i])))
		if head == "" || isAllowedQueryDraftDeclaration(head) || strings.HasPrefix(head, "set ") {
			continue
		}
		return statements[i]
	}
	return ""
}

func firstKQLContentLine(text string) string {
	for _, line := range strings.Split(text, "\n") {
		s := strings.TrimSpace(line)
		if s == "" || strings.HasPrefix(s, "//") || strings.HasPrefix(s, "#") {
			continue
		}
		return s
	}
	return ""
}

func splitKQLStatements(text string) []string {
	statements := []string{}
	var b strings.Builder
	inSingle := false
	inDouble := false
	inLineComment := false
	inBlockComment := false
	runes := []rune(text)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		next := rune(0)
		if i+1 < len(runes) {
			next = runes[i+1]
		}
		if inLineComment {
			b.WriteRune(ch)
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			b.WriteRune(ch)
			if ch == '*' && next == '/' {
				b.WriteRune(next)
				i++
				inBlockComment = false
			}
			continue
		}
		if inSingle {
			b.WriteRune(ch)
			if ch == '\'' {
				if next == '\'' {
					b.WriteRune(next)
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			b.WriteRune(ch)
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if ch == '/' && next == '/' {
			b.WriteRune(ch)
			b.WriteRune(next)
			i++
			inLineComment = true
			continue
		}
		if ch == '/' && next == '*' {
			b.WriteRune(ch)
			b.WriteRune(next)
			i++
			inBlockComment = true
			continue
		}
		if ch == '\'' {
			inSingle = true
			b.WriteRune(ch)
			continue
		}
		if ch == '"' {
			inDouble = true
			b.WriteRune(ch)
			continue
		}
		if ch == ';' {
			if stmt := strings.TrimSpace(b.String()); stmt != "" {
				statements = append(statements, stmt)
			}
			b.Reset()
			continue
		}
		b.WriteRune(ch)
	}
	if stmt := strings.TrimSpace(b.String()); stmt != "" {
		statements = append(statements, stmt)
	}
	return statements
}

func stripKQLCommentsAndStrings(text string) string {
	var b strings.Builder
	inSingle := false
	inDouble := false
	inLineComment := false
	inBlockComment := false
	runes := []rune(text)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		next := rune(0)
		if i+1 < len(runes) {
			next = runes[i+1]
		}
		if inLineComment {
			if ch == '\n' {
				inLineComment = false
				b.WriteRune('\n')
			} else {
				b.WriteRune(' ')
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && next == '/' {
				b.WriteString("  ")
				i++
				inBlockComment = false
			} else if ch == '\n' {
				b.WriteRune('\n')
			} else {
				b.WriteRune(' ')
			}
			continue
		}
		if inSingle {
			if ch == '\'' {
				if next == '\'' {
					b.WriteString("  ")
					i++
					continue
				}
				inSingle = false
			}
			b.WriteRune(' ')
			continue
		}
		if inDouble {
			if ch == '"' {
				inDouble = false
			}
			b.WriteRune(' ')
			continue
		}
		if ch == '/' && next == '/' {
			b.WriteString("  ")
			i++
			inLineComment = true
			continue
		}
		if ch == '/' && next == '*' {
			b.WriteString("  ")
			i++
			inBlockComment = true
			continue
		}
		if ch == '\'' {
			inSingle = true
			b.WriteRune(' ')
			continue
		}
		if ch == '"' {
			inDouble = true
			b.WriteRune(' ')
			continue
		}
		b.WriteRune(ch)
	}
	return b.String()
}

func (s *server) output() io.Writer {
	if s.stdout != nil {
		return s.stdout
	}
	return os.Stdout
}

func (s *server) runQueryCommand(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: kusto-cli query '<kql>'")
	}
	text, err := s.callTool(ctx, "kusto_query", mustMarshal(map[string]any{"cluster_uri": s.defaultClusterURI(), "database": s.cfg.database, "query": args[0]}))
	if err != nil {
		return err
	}
	return writeDirectText(os.Stdout, s.cfg.output, text)
}

func (s *server) runMgmtCommand(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: kusto-cli command '<management-command>'")
	}
	text, err := s.callTool(ctx, "kusto_command", mustMarshal(map[string]any{"cluster_uri": s.defaultClusterURI(), "database": s.cfg.database, "command": args[0]}))
	if err != nil {
		return err
	}
	return writeDirectText(os.Stdout, s.cfg.output, text)
}

func (s *server) runDatabaseCommand(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "list" {
		text, err := s.callTool(ctx, "kusto_list_entities", mustMarshal(map[string]any{"cluster_uri": s.defaultClusterURI(), "entity_type": "databases"}))
		if err != nil {
			return err
		}
		return writeDirectText(os.Stdout, s.cfg.output, text)
	}
	return fmt.Errorf("unknown databases command %q", args[0])
}

func (s *server) runTableCommand(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: kusto-cli tables <list|describe|sample> ...")
	}
	switch args[0] {
	case "list":
		text, err := s.callTool(ctx, "kusto_list_entities", mustMarshal(map[string]any{"cluster_uri": s.defaultClusterURI(), "database": s.cfg.database, "entity_type": "tables"}))
		if err != nil {
			return err
		}
		return writeDirectText(os.Stdout, s.cfg.output, text)
	case "describe":
		if len(args) < 2 {
			return errors.New("usage: kusto-cli tables describe <table-name>")
		}
		text, err := s.callTool(ctx, "kusto_describe_database_entity", mustMarshal(map[string]any{"cluster_uri": s.defaultClusterURI(), "database": s.cfg.database, "entity_type": "table", "entity_name": args[1]}))
		if err != nil {
			return err
		}
		return writeDirectText(os.Stdout, s.cfg.output, text)
	case "sample":
		if len(args) < 2 {
			return errors.New("usage: kusto-cli tables sample <table-name> [sample-size]")
		}
		size := 10
		if len(args) > 2 {
			if v, err := strconv.Atoi(args[2]); err == nil {
				size = v
			}
		}
		text, err := s.callTool(ctx, "kusto_sample_entity", mustMarshal(map[string]any{"cluster_uri": s.defaultClusterURI(), "database": s.cfg.database, "entity_type": "table", "entity_name": args[1], "sample_size": size}))
		if err != nil {
			return err
		}
		return writeDirectText(os.Stdout, s.cfg.output, text)
	default:
		return fmt.Errorf("unknown tables command %q", args[0])
	}
}

func (s *server) runEntityCommand(ctx context.Context, args []string) error {
	if len(args) < 2 || args[0] != "list" {
		return errors.New("usage: kusto-cli entities list <entity-type>")
	}
	text, err := s.callTool(ctx, "kusto_list_entities", mustMarshal(map[string]any{"cluster_uri": s.defaultClusterURI(), "database": s.cfg.database, "entity_type": args[1]}))
	if err != nil {
		return err
	}
	return writeDirectText(os.Stdout, s.cfg.output, text)
}

func (s *server) runServiceCommand(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "list" {
		return writeOutput(os.Stdout, s.cfg.output, s.knownServices)
	}
	return fmt.Errorf("unknown services command %q", args[0])
}

func (s *server) runDeeplinkCommand(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: kusto-cli deeplink '<kql>'")
	}
	text, err := s.callTool(ctx, "kusto_deeplink_from_query", mustMarshal(map[string]any{"cluster_uri": s.defaultClusterURI(), "database": s.cfg.database, "query": args[0]}))
	if err != nil {
		return err
	}
	return writeDirectText(os.Stdout, s.cfg.output, text)
}

func (s *server) runQueryPlanCommand(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: kusto-cli queryplan '<kql>'")
	}
	text, err := s.callTool(ctx, "kusto_show_queryplan", mustMarshal(map[string]any{"cluster_uri": s.defaultClusterURI(), "database": s.cfg.database, "query": args[0]}))
	if err != nil {
		return err
	}
	return writeDirectText(os.Stdout, s.cfg.output, text)
}

func (s *server) runDiagnosticsCommand(ctx context.Context, args []string) error {
	text, err := s.callTool(ctx, "kusto_diagnostics", mustMarshal(map[string]any{"cluster_uri": s.defaultClusterURI(), "database": s.cfg.database}))
	if err != nil {
		return err
	}
	return writeDirectText(os.Stdout, s.cfg.output, text)
}

func (s *server) runAPICommand(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: kusto-cli api <tools|schema|call>")
	}
	switch args[0] {
	case "tools":
		return writeOutput(os.Stdout, s.cfg.output, map[string]any{"tools": toolDefinitions()})
	case "schema":
		if len(args) < 2 {
			return errors.New("usage: kusto-cli api schema <tool-name>")
		}
		tool, err := findToolDefinition(args[1])
		if err != nil {
			return err
		}
		return writeOutput(os.Stdout, s.cfg.output, tool)
	case "call":
		if len(args) < 3 {
			return errors.New("usage: kusto-cli api call <tool-name> '<json-arguments>'")
		}
		text, err := s.callTool(ctx, args[1], json.RawMessage(args[2]))
		if err != nil {
			return err
		}
		return writeDirectText(os.Stdout, s.cfg.output, text)
	default:
		return fmt.Errorf("unknown api command %q", args[0])
	}
}

func printKustoUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  kusto-cli --service-uri <cluster> --database <db> ask [--include-samples] [--shots-table <table>] [--shots-limit N] [--validate-plan] [--repair] [--max-repair-attempts N] [--execute] [--max-rows N] '<natural-language prompt>'
  kusto-cli --target <alias> ask [--include-samples] [--shots-table <table>] [--shots-limit N] [--validate-plan] [--repair] [--max-repair-attempts N] [--execute] [--max-rows N] '<natural-language prompt>'
  kusto-cli --service-uri <cluster> --database <db> query '<kql>'
  kusto-cli databases list
  kusto-cli tables list
  kusto-cli tables describe <table>
  kusto-cli tables sample <table> [size]
  kusto-cli command '.show tables'
  kusto-cli deeplink '<kql>'
  kusto-cli queryplan '<kql>'
  kusto-cli diagnostics
  kusto-cli api tools|schema|call ...
  kusto-cli auth status
  kusto-cli config show`)
}

func findToolDefinition(name string) (map[string]any, error) {
	for _, tool := range toolDefinitions() {
		if toolName, _ := tool["name"].(string); toolName == name {
			return tool, nil
		}
	}
	return nil, fmt.Errorf("tool %q not found", name)
}

func (s *server) runAuthCommand(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: kusto-cli auth <status|token>")
	}
	switch args[0] {
	case "status":
		_, err := s.auth.get(ctx)
		return writeOutput(os.Stdout, s.cfg.output, map[string]any{"authenticated": err == nil, "mode": s.cfg.authMode, "token_env": s.cfg.tokenEnv, "error": errorString(err)})
	case "token":
		tok, err := s.auth.get(ctx)
		if err != nil {
			return err
		}
		return writeOutput(os.Stdout, s.cfg.output, map[string]any{"accessToken": tok, "tokenType": "Bearer"})
	default:
		return fmt.Errorf("unknown auth command %q (expected: status, token)", args[0])
	}
}

func writePretty(w io.Writer, v any) error {
	return writeOutput(w, "json", v)
}

func writeOutput(w io.Writer, format string, v any) error {
	switch strings.ToLower(format) {
	case "", "json":
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	case "table":
		return writeHuman(w, v, "")
	case "tsv", "plain":
		return writeTSV(w, v)
	default:
		return fmt.Errorf("unknown output format %q", format)
	}
}

func writeQueryDraft(w io.Writer, format string, draft queryDraft) error {
	switch strings.ToLower(format) {
	case "", "json":
		return writeOutput(w, "json", draft)
	case "table":
		fmt.Fprintf(w, "Target: %s / %s\n", draft.Target.ClusterURI, draft.Target.Database)
		fmt.Fprintf(w, "Prompt: %s\n\n", draft.Prompt)
		if draft.ClarificationRequired {
			fmt.Fprintf(w, "Clarification Required: %s\n\n", draft.ClarificationQuestion)
		}
		fmt.Fprintf(w, "Query:\n%s\n\n", draft.Query)
		writeBullets(w, "Assumptions", draft.Assumptions)
		writeBullets(w, "Warnings", draft.Warnings)
		if len(draft.Examples) > 0 {
			fmt.Fprintln(w, "Examples sent to model provider:")
			for _, example := range draft.Examples {
				label := firstNonEmpty(example.Name, example.Intent, "example")
				fmt.Fprintf(w, "- %s/%s: %s\n", example.Source, label, oneLine(example.Intent))
			}
		}
		fmt.Fprintf(w, "Data Disclosure Policy: %s\n", draft.DataDisclosure.Mode)
		fmt.Fprintf(w, "Sent to model provider: schema=%t docstrings=%t shots=%t sample_rows=%t query_results=%t\n", draft.DataDisclosure.Sent.Schema, draft.DataDisclosure.Sent.Docstrings, draft.DataDisclosure.Sent.Shots, draft.DataDisclosure.Sent.SampleRows, draft.DataDisclosure.Sent.QueryResults)
		if draft.ModelSafety != nil && (draft.ModelSafety.Classification != "" || draft.ModelSafety.Reason != "") {
			fmt.Fprintf(w, "Model Safety: %s (advisory=%t)\n", draft.ModelSafety.Classification, draft.ModelSafety.Advisory)
			if draft.ModelSafety.Reason != "" {
				fmt.Fprintf(w, "Model Safety Reason: %s\n", draft.ModelSafety.Reason)
			}
		}
		fmt.Fprintf(w, "Validation: %s (read_only=%t bounded=%t safe_for_execution=%t)\n", draft.Validation.Status, draft.Validation.ReadOnly, draft.Validation.Bounded, draft.Validation.SafeForExecution)
		for _, check := range draft.Validation.Checks {
			status := "failed"
			if check.Passed {
				status = "passed"
			}
			if check.Severity != "" {
				status += "/" + check.Severity
			}
			if check.Message == "" {
				fmt.Fprintf(w, "- %s: %s\n", check.Name, status)
				continue
			}
			fmt.Fprintf(w, "- %s: %s (%s)\n", check.Name, status, check.Message)
		}
		if draft.Validation.QueryPlan != nil {
			fmt.Fprintf(w, "Query Plan Validation: %s", draft.Validation.QueryPlan.Status)
			if draft.Validation.QueryPlan.Error != "" {
				fmt.Fprintf(w, " (%s)", draft.Validation.QueryPlan.Error)
			} else if draft.Validation.QueryPlan.Message != "" {
				fmt.Fprintf(w, " (%s)", draft.Validation.QueryPlan.Message)
			}
			fmt.Fprintln(w)
		}
		if len(draft.RepairHistory) > 0 {
			fmt.Fprintln(w, "Repair History:")
			for _, pass := range draft.RepairHistory {
				fmt.Fprintf(w, "- attempt %d: %s", pass.Attempt, pass.Status)
				if pass.Trigger != "" {
					fmt.Fprintf(w, " (%s)", pass.Trigger)
				}
				if pass.OutputQuery != "" {
					fmt.Fprintf(w, " -> %s", oneLine(pass.OutputQuery))
				}
				if pass.Error != "" {
					fmt.Fprintf(w, " error=%s", oneLine(pass.Error))
				}
				fmt.Fprintln(w)
			}
		}
		if draft.Execution.Executed {
			fmt.Fprintf(w, "Execution: executed (%s)\n", draft.Execution.Reason)
		} else {
			fmt.Fprintf(w, "Execution: not executed (%s)\n", draft.Execution.Reason)
		}
		if draft.Execution.Status != "" {
			fmt.Fprintf(w, "Execution Status: %s\n", draft.Execution.Status)
		}
		if draft.Execution.MaxRecords > 0 {
			fmt.Fprintf(w, "Execution Max Records: %d\n", draft.Execution.MaxRecords)
		}
		if draft.Execution.Error != "" {
			fmt.Fprintf(w, "Execution Error: %s\n", draft.Execution.Error)
		}
		if draft.Execution.Result != nil {
			fmt.Fprintln(w, "Execution Result:")
			return writeKustoTable(w, *draft.Execution.Result)
		}
		return nil
	case "tsv", "plain":
		fmt.Fprintf(w, "format\t%s\n", oneLine(draft.Format))
		fmt.Fprintf(w, "target.cluster_uri\t%s\n", oneLine(draft.Target.ClusterURI))
		fmt.Fprintf(w, "target.database\t%s\n", oneLine(draft.Target.Database))
		fmt.Fprintf(w, "prompt\t%s\n", oneLine(draft.Prompt))
		fmt.Fprintf(w, "query\t%s\n", oneLine(draft.Query))
		if draft.ClarificationRequired {
			fmt.Fprintf(w, "clarification_required\t%t\n", draft.ClarificationRequired)
			fmt.Fprintf(w, "clarification_question\t%s\n", oneLine(draft.ClarificationQuestion))
		}
		fmt.Fprintf(w, "assumptions\t%s\n", oneLine(strings.Join(draft.Assumptions, "; ")))
		fmt.Fprintf(w, "warnings\t%s\n", oneLine(strings.Join(draft.Warnings, "; ")))
		if len(draft.Examples) > 0 {
			fmt.Fprintf(w, "examples\t%s\n", oneLine(string(mustJSON(draft.Examples))))
		}
		fmt.Fprintf(w, "data_disclosure_policy.mode\t%s\n", oneLine(draft.DataDisclosure.Mode))
		fmt.Fprintf(w, "data_disclosure_policy.sent.schema\t%t\n", draft.DataDisclosure.Sent.Schema)
		fmt.Fprintf(w, "data_disclosure_policy.sent.docstrings\t%t\n", draft.DataDisclosure.Sent.Docstrings)
		fmt.Fprintf(w, "data_disclosure_policy.sent.shots\t%t\n", draft.DataDisclosure.Sent.Shots)
		fmt.Fprintf(w, "data_disclosure_policy.sent.sample_rows\t%t\n", draft.DataDisclosure.Sent.SampleRows)
		fmt.Fprintf(w, "data_disclosure_policy.sent.query_results\t%t\n", draft.DataDisclosure.Sent.QueryResults)
		if draft.ModelSafety != nil && (draft.ModelSafety.Classification != "" || draft.ModelSafety.Reason != "") {
			fmt.Fprintf(w, "model_safety.classification\t%s\n", oneLine(draft.ModelSafety.Classification))
			fmt.Fprintf(w, "model_safety.reason\t%s\n", oneLine(draft.ModelSafety.Reason))
			fmt.Fprintf(w, "model_safety.advisory\t%t\n", draft.ModelSafety.Advisory)
		}
		fmt.Fprintf(w, "validation.status\t%s\n", oneLine(draft.Validation.Status))
		fmt.Fprintf(w, "validation.read_only\t%t\n", draft.Validation.ReadOnly)
		fmt.Fprintf(w, "validation.bounded\t%t\n", draft.Validation.Bounded)
		fmt.Fprintf(w, "validation.safe_for_execution\t%t\n", draft.Validation.SafeForExecution)
		fmt.Fprintf(w, "validation.warnings\t%s\n", oneLine(strings.Join(draft.Validation.Warnings, "; ")))
		fmt.Fprintf(w, "validation.errors\t%s\n", oneLine(strings.Join(draft.Validation.Errors, "; ")))
		if draft.Validation.QueryPlan != nil {
			fmt.Fprintf(w, "validation.query_plan.status\t%s\n", oneLine(draft.Validation.QueryPlan.Status))
			if draft.Validation.QueryPlan.Error != "" {
				fmt.Fprintf(w, "validation.query_plan.error\t%s\n", oneLine(draft.Validation.QueryPlan.Error))
			}
			if draft.Validation.QueryPlan.Message != "" {
				fmt.Fprintf(w, "validation.query_plan.message\t%s\n", oneLine(draft.Validation.QueryPlan.Message))
			}
		}
		if len(draft.RepairHistory) > 0 {
			fmt.Fprintf(w, "repair_history\t%s\n", oneLine(string(mustJSON(draft.RepairHistory))))
		}
		fmt.Fprintf(w, "execution.executed\t%t\n", draft.Execution.Executed)
		fmt.Fprintf(w, "execution.reason\t%s\n", oneLine(draft.Execution.Reason))
		if draft.Execution.Status != "" {
			fmt.Fprintf(w, "execution.status\t%s\n", oneLine(draft.Execution.Status))
		}
		if draft.Execution.MaxRecords > 0 {
			fmt.Fprintf(w, "execution.max_records\t%d\n", draft.Execution.MaxRecords)
		}
		if draft.Execution.Error != "" {
			fmt.Fprintf(w, "execution.error\t%s\n", oneLine(draft.Execution.Error))
		}
		if draft.Execution.Result != nil {
			fmt.Fprintf(w, "execution.result\t%s\n", oneLine(string(mustJSON(draft.Execution.Result))))
		}
		return nil
	default:
		return fmt.Errorf("unknown output format %q", format)
	}
}

func writeBullets(w io.Writer, title string, items []string) {
	fmt.Fprintf(w, "%s:\n", title)
	if len(items) == 0 {
		fmt.Fprintln(w, "- (none)")
		return
	}
	for _, item := range items {
		fmt.Fprintf(w, "- %s\n", item)
	}
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, "\t", `\t`)
	return s
}

func writeDirectText(w io.Writer, format string, text string) error {
	var kr kustoResponse
	if err := json.Unmarshal([]byte(text), &kr); err == nil && kr.Format == "kusto_response" {
		switch strings.ToLower(format) {
		case "table":
			return writeKustoTable(w, kr)
		case "tsv", "plain":
			return writeKustoTSV(w, kr)
		default:
			return writeOutput(w, "json", kr)
		}
	}
	var v any
	if err := json.Unmarshal([]byte(text), &v); err == nil {
		return writeOutput(w, format, v)
	}
	_, err := fmt.Fprintln(w, text)
	return err
}

func writeKustoTable(w io.Writer, kr kustoResponse) error {
	if len(kr.Data.Columns) == 0 {
		fmt.Fprintln(w, "(no columns)")
		return nil
	}
	for i, c := range kr.Data.Columns {
		if i > 0 {
			fmt.Fprint(w, "\t")
		}
		fmt.Fprint(w, c.ColumnName)
	}
	fmt.Fprintln(w)
	for _, row := range kr.Data.Rows {
		for i := range kr.Data.Columns {
			if i > 0 {
				fmt.Fprint(w, "\t")
			}
			if i < len(row) {
				fmt.Fprint(w, row[i])
			}
		}
		fmt.Fprintln(w)
	}
	return nil
}

func writeKustoTSV(w io.Writer, kr kustoResponse) error { return writeKustoTable(w, kr) }

func writeHuman(w io.Writer, v any, indent string) error {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			fmt.Fprintf(w, "%s%s: ", indent, k)
			switch val.(type) {
			case map[string]any, []any:
				fmt.Fprintln(w)
				if err := writeHuman(w, val, indent+"  "); err != nil {
					return err
				}
			default:
				fmt.Fprintln(w, val)
			}
		}
	case []any:
		for i, item := range x {
			fmt.Fprintf(w, "%s- [%d]\n", indent, i)
			if err := writeHuman(w, item, indent+"  "); err != nil {
				return err
			}
		}
	default:
		fmt.Fprintln(w, x)
	}
	return nil
}

func writeTSV(w io.Writer, v any) error {
	b, _ := json.Marshal(v)
	var generic any
	if err := json.Unmarshal(b, &generic); err != nil {
		return err
	}
	if m, ok := generic.(map[string]any); ok {
		for k, val := range m {
			fmt.Fprintf(w, "%s\t%v\n", k, val)
		}
		return nil
	}
	fmt.Fprintln(w, generic)
	return nil
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}

func visitedFlags() map[string]bool {
	seen := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	return seen
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func configFilePath(app string) (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, app, "config.json"), nil
}

func loadConfigMap(app string) map[string]string {
	path, err := configFilePath(app)
	if err != nil {
		return map[string]string{}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return map[string]string{}
	}
	var cfg map[string]string
	if json.Unmarshal(b, &cfg) != nil {
		return map[string]string{}
	}
	return cfg
}

func saveConfigMap(app string, cfg map[string]string) error {
	path, err := configFilePath(app)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(cfg, "", "  ")
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o600)
}

func applyFileConfig(cfg *config, visited map[string]bool) {
	fileCfg := loadConfigMap("kusto-cli")
	if !visited["service-uri"] && !visited["service-url"] && os.Getenv("KUSTO_SERVICE_URI") == "" && fileCfg["service-uri"] != "" {
		cfg.serviceURI = fileCfg["service-uri"]
	}
	if !visited["database"] && os.Getenv("KUSTO_SERVICE_DEFAULT_DB") == "" && fileCfg["database"] != "" {
		cfg.database = fileCfg["database"]
		cfg.databaseConfigured = true
	}
	if !visited["known-services"] && os.Getenv("KUSTO_KNOWN_SERVICES") == "" && fileCfg["known-services"] != "" {
		cfg.knownServices = fileCfg["known-services"]
	}
	if !visited["target"] && !visited["target-alias"] && os.Getenv("KUSTO_TARGET") == "" && os.Getenv("KUSTO_TARGET_ALIAS") == "" {
		if target := firstNonEmpty(fileCfg["target"], fileCfg["target-alias"]); target != "" {
			cfg.targetAlias = target
		}
	}
	if !visited["tenant"] && fileCfg["tenant"] != "" {
		cfg.tenant = fileCfg["tenant"]
	}
	if !visited["auth"] && fileCfg["auth"] != "" {
		cfg.authMode = fileCfg["auth"]
	}
	if !visited["output"] && !visited["o"] && os.Getenv("KUSTO_OUTPUT") == "" && fileCfg["output"] != "" {
		cfg.output = fileCfg["output"]
	}
	if !visited["model-provider"] && os.Getenv("KUSTO_MODEL_PROVIDER") == "" && fileCfg["model-provider"] != "" {
		cfg.modelProvider = fileCfg["model-provider"]
	}
	if !visited["model-endpoint"] && os.Getenv("KUSTO_MODEL_ENDPOINT") == "" && fileCfg["model-endpoint"] != "" {
		cfg.modelEndpoint = fileCfg["model-endpoint"]
	}
	if !visited["model"] && os.Getenv("KUSTO_MODEL") == "" && fileCfg["model"] != "" {
		cfg.modelName = fileCfg["model"]
	}
	if !visited["model-api-key-env"] && os.Getenv("KUSTO_MODEL_API_KEY_ENV") == "" && fileCfg["model-api-key-env"] != "" {
		cfg.modelAPIKeyEnv = fileCfg["model-api-key-env"]
	}
}

func runConfigCommand(app string, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: config <path|show|set|unset>")
	}
	cfg := loadConfigMap(app)
	switch args[0] {
	case "path":
		path, err := configFilePath(app)
		if err != nil {
			return err
		}
		fmt.Fprintln(os.Stdout, path)
		return nil
	case "show":
		return writeOutput(os.Stdout, "json", cfg)
	case "set":
		if len(args) < 3 {
			return errors.New("usage: config set <key> <value>")
		}
		cfg[args[1]] = args[2]
		return saveConfigMap(app, cfg)
	case "unset":
		if len(args) < 2 {
			return errors.New("usage: config unset <key>")
		}
		delete(cfg, args[1])
		return saveConfigMap(app, cfg)
	default:
		return fmt.Errorf("unknown config command %q", args[0])
	}
}

func runCompletionCommand(name string, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: completion <bash|zsh|fish>")
	}
	commands := "ask query command databases tables entities services deeplink queryplan diagnostics api auth config completion"
	switch args[0] {
	case "bash":
		fmt.Fprintf(os.Stdout, "complete -W %q %s\n", commands, name)
	case "zsh":
		fmt.Fprintf(os.Stdout, "#compdef %s\n_arguments '1:command:(%s)'\n", name, commands)
	case "fish":
		for _, c := range strings.Fields(commands) {
			fmt.Fprintf(os.Stdout, "complete -c %s -f -a %s\n", name, c)
		}
	default:
		return fmt.Errorf("unknown shell %q", args[0])
	}
	return nil
}

func (s *server) serveStdio(ctx context.Context, r io.Reader, w io.Writer) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 32*1024*1024)
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var msg rpcMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			writeRPCError(bw, nil, -32700, "Parse error", err.Error())
			continue
		}
		if len(msg.ID) == 0 {
			// notifications/initialized etc. need no response.
			continue
		}
		result, err := s.handle(ctx, msg)
		if err != nil {
			writeToolError(bw, msg.ID, err)
			continue
		}
		writeRPCResult(bw, msg.ID, result)
	}
	return scanner.Err()
}

func (s *server) handle(ctx context.Context, msg rpcMessage) (any, error) {
	switch msg.Method {
	case "initialize":
		return s.initializeResult(msg.Params), nil
	case "tools/list":
		return map[string]any{"tools": toolDefinitions()}, nil
	case "tools/call":
		var params toolCallParams
		if len(msg.Params) == 0 {
			return nil, errors.New("tools/call missing params")
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, err
		}
		text, err := s.callTool(ctx, params.Name, params.Arguments)
		if err != nil {
			return toolResult(fmt.Sprintf("Error: %v", err), true), nil
		}
		return toolResult(text, false), nil
	default:
		return nil, fmt.Errorf("unsupported method %q", msg.Method)
	}
}

func (s *server) initializeResult(params json.RawMessage) any {
	protocol := defaultProtocolVer
	var p struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if len(params) > 0 && json.Unmarshal(params, &p) == nil && p.ProtocolVersion != "" {
		protocol = p.ProtocolVersion
	}
	instructions := "Standalone Kusto CLI — query and explore Azure Data Explorer / Microsoft Fabric Eventhouse (Kusto) clusters."
	if len(s.knownServices) > 0 {
		var b strings.Builder
		b.WriteString(instructions)
		b.WriteString("\n\nConfigured clusters (use these cluster_uri values; omit database to use the listed default):")
		for _, svc := range s.knownServices {
			b.WriteString("\n- ")
			b.WriteString(svc.ServiceURI)
			if svc.DefaultDatabase != "" {
				b.WriteString(" (database: ")
				b.WriteString(svc.DefaultDatabase)
				b.WriteString(")")
			}
			if svc.Description != "" {
				b.WriteString(" — ")
				b.WriteString(svc.Description)
			}
		}
		instructions = b.String()
	}
	return map[string]any{
		"protocolVersion": protocol,
		"capabilities":    map[string]any{"tools": map[string]any{}},
		"serverInfo":      map[string]any{"name": "kusto-cli", "version": version.Version},
		"instructions":    instructions,
	}
}

func (s *server) callTool(ctx context.Context, name string, raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		raw = []byte("{}")
	}
	switch name {
	case "kusto_known_services":
		return marshalText(s.knownServices)
	case "kusto_query":
		var a queryArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		if err := validateQuery(a.Query); err != nil {
			return "", err
		}
		resp, err := s.execute(ctx, a.ClusterURI, a.Database, a.Query, "query", true, a.ClientRequestProperties)
		return marshalKusto(resp, err)
	case "kusto_command":
		var a commandArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		if err := validateCommand(a.Command); err != nil {
			return "", err
		}
		readonly := isSafeManagementCommand(a.Command)
		if !readonly && !s.cfg.allowWrite {
			if s.cfg.dryRun {
				return marshalText(map[string]any{"dry_run": true, "tool": "kusto_command", "command": a.Command})
			}
			return "", errors.New("destructive management commands require --allow-write")
		}
		resp, err := s.execute(ctx, a.ClusterURI, a.Database, a.Command, "mgmt", readonly, a.ClientRequestProperties)
		return marshalKusto(resp, err)
	case "kusto_deeplink_from_query":
		var a deeplinkArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		if a.ClusterURI == "" {
			a.ClusterURI = s.defaultClusterURI()
		}
		if a.Database == "" {
			a.Database = s.defaultDatabaseFor(a.ClusterURI, "")
		}
		if a.ClusterURI == "" || a.Database == "" || a.Query == "" {
			return "", errors.New("cluster_uri, database, and query are required")
		}
		return buildDeeplink(a.ClusterURI, a.Database, a.Query), nil
	case "kusto_list_entities":
		var a listEntitiesArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		cmd, db, err := listEntitiesCommand(a.EntityType, a.Database)
		if err != nil {
			return "", err
		}
		resp, err := s.execute(ctx, a.ClusterURI, db, cmd, "mgmt", true, a.ClientRequestProperties)
		return marshalKusto(resp, err)
	case "kusto_describe_database":
		var a describeDatabaseArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		db := s.defaultDatabaseFor(a.ClusterURI, a.Database)
		cmd := ".show databases entities with (showObfuscatedStrings=true) | where DatabaseName == '" + kqlEscapeString(db) + "' | project EntityName, EntityType, Folder, DocString, CslInputSchema, Content, CslOutputSchema"
		resp, err := s.execute(ctx, a.ClusterURI, db, cmd, "mgmt", true, a.ClientRequestProperties)
		return marshalKusto(resp, err)
	case "kusto_describe_database_entity":
		var a describeEntityArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		cmd, err := describeEntityCommand(a.EntityType, a.EntityName)
		if err != nil {
			return "", err
		}
		resp, err := s.execute(ctx, a.ClusterURI, a.Database, cmd, "mgmt", true, a.ClientRequestProperties)
		return marshalKusto(resp, err)
	case "kusto_sample_entity":
		var a sampleEntityArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		if a.SampleSize <= 0 {
			a.SampleSize = 10
		}
		query, err := sampleEntityQuery(a.EntityType, a.EntityName, a.SampleSize)
		if err != nil {
			return "", err
		}
		resp, err := s.execute(ctx, a.ClusterURI, a.Database, query, "query", true, a.ClientRequestProperties)
		return marshalKusto(resp, err)
	case "kusto_graph_query":
		var a graphQueryArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		if a.GraphName == "" || a.Query == "" {
			return "", errors.New("graph_name and query are required")
		}
		query := "graph('" + kqlEscapeString(a.GraphName) + "') " + strings.TrimSpace(a.Query)
		resp, err := s.execute(ctx, a.ClusterURI, a.Database, query, "query", true, a.ClientRequestProperties)
		return marshalKusto(resp, err)
	case "kusto_ingest_inline_into_table":
		var a ingestArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		if a.TableName == "" || a.DataCommaSeparator == "" {
			return "", errors.New("table_name and data_comma_separator are required")
		}
		cmd := ".ingest inline into table " + kqlEscapeEntityName(a.TableName) + " <| " + a.DataCommaSeparator
		if !s.cfg.allowWrite {
			if s.cfg.dryRun {
				return marshalText(map[string]any{"dry_run": true, "tool": "kusto_ingest_inline_into_table", "table_name": a.TableName})
			}
			return "", errors.New("inline ingestion requires --allow-write")
		}
		resp, err := s.execute(ctx, a.ClusterURI, a.Database, cmd, "mgmt", false, a.ClientRequestProperties)
		return marshalKusto(resp, err)
	case "kusto_get_shots":
		var a getShotsArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		if a.ShotsTableName == "" {
			a.ShotsTableName = os.Getenv("KUSTO_SHOTS_TABLE")
		}
		if a.ShotsTableName == "" {
			return "[]", nil
		}
		if a.SampleSize <= 0 {
			a.SampleSize = defaultQueryDraftShotsLimit
		}
		query := queryDraftShotsQuery(a.ShotsTableName, a.Prompt, a.SampleSize)
		resp, err := s.execute(ctx, a.ClusterURI, a.Database, query, "query", true, a.ClientRequestProperties)
		return marshalKusto(resp, err)
	case "kusto_show_queryplan":
		var a queryArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		if err := validateQuery(a.Query); err != nil {
			return "", err
		}
		resp, err := s.execute(ctx, a.ClusterURI, a.Database, ".show queryplan <| "+strings.TrimSpace(a.Query), "mgmt", true, a.ClientRequestProperties)
		return marshalKusto(resp, err)
	case "kusto_diagnostics":
		var a diagnosticsArgs
		if err := decodeArgs(raw, &a); err != nil {
			return "", err
		}
		res := map[string]any{}
		for section, cmd := range diagnosticsCommands() {
			resp, err := s.execute(ctx, a.ClusterURI, a.Database, cmd, "mgmt", true, a.ClientRequestProperties)
			if err != nil {
				res[section] = map[string]string{"error": err.Error()}
				continue
			}
			res[section] = rowsToDicts(resp)
		}
		return marshalText(res)
	default:
		return "", fmt.Errorf("unknown tool %q", name)
	}
}

type baseArgs struct {
	ClusterURI              string         `json:"cluster_uri"`
	Database                string         `json:"database"`
	ClientRequestProperties map[string]any `json:"client_request_properties"`
}
type queryArgs struct {
	baseArgs
	Query string `json:"query"`
}
type commandArgs struct {
	baseArgs
	Command string `json:"command"`
}
type deeplinkArgs struct {
	ClusterURI string `json:"cluster_uri"`
	Database   string `json:"database"`
	Query      string `json:"query"`
}
type listEntitiesArgs struct {
	baseArgs
	EntityType string `json:"entity_type"`
}
type describeDatabaseArgs struct{ baseArgs }
type describeEntityArgs struct {
	baseArgs
	EntityName string `json:"entity_name"`
	EntityType string `json:"entity_type"`
}
type sampleEntityArgs struct {
	baseArgs
	EntityName string `json:"entity_name"`
	EntityType string `json:"entity_type"`
	SampleSize int    `json:"sample_size"`
}
type graphQueryArgs struct {
	baseArgs
	GraphName string `json:"graph_name"`
	Query     string `json:"query"`
}
type ingestArgs struct {
	baseArgs
	TableName          string `json:"table_name"`
	DataCommaSeparator string `json:"data_comma_separator"`
}
type getShotsArgs struct {
	baseArgs
	Prompt            string `json:"prompt"`
	EmbeddingEndpoint string `json:"embedding_endpoint"`
	ShotsTableName    string `json:"shots_table_name"`
	SampleSize        int    `json:"sample_size"`
}
type diagnosticsArgs struct{ baseArgs }

func marshalKusto(resp kustoResponse, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return marshalText(resp)
}

func (s *server) execute(ctx context.Context, clusterURI, database, csl, kind string, readonly bool, crp map[string]any) (kustoResponse, error) {
	var zero kustoResponse
	clusterURI = strings.TrimSpace(clusterURI)
	if clusterURI == "" {
		clusterURI = s.defaultClusterURI()
	}
	if clusterURI == "" {
		return zero, errors.New("cluster_uri is required and no default --service-uri is configured")
	}
	clusterURI = strings.TrimRight(clusterURI, "/")
	if _, err := url.ParseRequestURI(clusterURI); err != nil {
		return zero, fmt.Errorf("cluster_uri is not a valid URL: %w", err)
	}
	database = s.defaultDatabaseFor(clusterURI, database)
	if database == "" {
		database = defaultKustoDB
	}
	path := "/v2/rest/query"
	if kind == "mgmt" {
		path = "/v1/rest/mgmt"
	}
	token, err := s.auth.get(ctx)
	if err != nil {
		return zero, err
	}
	props, err := buildRequestProperties(readonly, crp)
	if err != nil {
		return zero, err
	}
	propsJSON, _ := json.Marshal(props)
	body, _ := json.Marshal(map[string]any{"db": database, "csl": csl, "properties": string(propsJSON)})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, clusterURI+path, bytes.NewReader(body))
	if err != nil {
		return zero, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", s.cfg.userAgent)
	req.Header.Set("x-ms-client-request-id", "KUSTO_CLI."+kind+":"+randomID())
	debugf(s.cfg.debug, "POST %s db=%s readonly=%t csl=%q", path, database, readonly, summarize(csl, 120))
	resp, err := s.hc.Do(req)
	if err != nil {
		return zero, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return zero, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return zero, fmt.Errorf("Kusto HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return parseKustoResponse(b)
}

func buildRequestProperties(readonly bool, user map[string]any) (map[string]any, error) {
	options := map[string]any{}
	if readonly {
		options["request_readonly"] = true
		options["request_readonly_hardline"] = true
	}
	for k, v := range user {
		lk := strings.ToLower(k)
		if lk == "request_readonly" || lk == "request_readonly_hardline" {
			return nil, fmt.Errorf("client request property %q is security-sensitive and cannot be overridden", k)
		}
		options[k] = v
	}
	return map[string]any{"Options": options}, nil
}

func parseKustoResponse(b []byte) (kustoResponse, error) {
	trimmed := bytes.TrimSpace(b)
	if len(trimmed) == 0 {
		return kustoResponse{}, errors.New("empty Kusto response")
	}
	if trimmed[0] == '[' {
		return parseKustoV2Response(trimmed)
	}
	return parseKustoV1Response(trimmed)
}

func parseKustoV2Response(b []byte) (kustoResponse, error) {
	var zero kustoResponse
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var frames []kustoFrame
	if err := dec.Decode(&frames); err != nil {
		return zero, fmt.Errorf("Kusto response was not valid JSON: %w", err)
	}
	for _, f := range frames {
		if f.FrameType == "DataSetCompletion" && f.HasErrors {
			return zero, fmt.Errorf("Kusto query completed with errors: %s", string(mustJSON(f.OneAPIErrors)))
		}
	}
	var table *kustoFrame
	for i := range frames {
		if frames[i].FrameType == "DataTable" && strings.EqualFold(frames[i].TableKind, "PrimaryResult") {
			table = &frames[i]
			break
		}
	}
	if table == nil {
		for i := range frames {
			if frames[i].FrameType == "DataTable" && !strings.HasPrefix(frames[i].TableName, "@") && !strings.Contains(frames[i].TableName, "Completion") {
				table = &frames[i]
				break
			}
		}
	}
	if table == nil {
		return zero, errors.New("Kusto response did not contain a primary result table")
	}
	return makeKustoResponse(table.Columns, table.Rows), nil
}

func parseKustoV1Response(b []byte) (kustoResponse, error) {
	var zero kustoResponse
	type table struct {
		TableName string        `json:"TableName"`
		Columns   []kustoColumn `json:"Columns"`
		Rows      [][]any       `json:"Rows"`
	}
	var envelope struct {
		Tables []table `json:"Tables"`
		Error  any     `json:"error"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&envelope); err != nil {
		return zero, fmt.Errorf("Kusto response was not valid JSON: %w", err)
	}
	if envelope.Error != nil {
		return zero, fmt.Errorf("Kusto returned error: %s", string(mustJSON(envelope.Error)))
	}
	if len(envelope.Tables) == 0 {
		return zero, errors.New("Kusto response did not contain tables")
	}
	t := envelope.Tables[0]
	return makeKustoResponse(t.Columns, t.Rows), nil
}

func makeKustoResponse(columns []kustoColumn, rows [][]any) kustoResponse {
	for i := range columns {
		if columns[i].ColumnType == "" && columns[i].DataType != "" {
			columns[i].ColumnType = strings.ToLower(columns[i].DataType)
		}
		columns[i].DataType = ""
	}
	var kr kustoResponse
	kr.Format = "kusto_response"
	kr.Data.Columns = columns
	kr.Data.Rows = rows
	return kr
}

func (tp *tokenProvider) get(ctx context.Context) (string, error) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	if tp.mode == "none" {
		return "", errors.New("auth mode none cannot call Kusto; provide --auth env/azcli/auto")
	}
	if tp.token != "" && time.Until(tp.expires) > 5*time.Minute {
		return tp.token, nil
	}
	if tp.mode == "env" || tp.mode == "auto" {
		if tok := strings.TrimSpace(os.Getenv(tp.tokenEnv)); tok != "" {
			tp.token = tok
			tp.expires = time.Now().Add(30 * time.Minute)
			return tp.token, nil
		}
		if tp.mode == "env" {
			return "", fmt.Errorf("%s is empty", tp.tokenEnv)
		}
	}
	if tp.mode == "azcli" || tp.mode == "auto" {
		args := []string{"account", "get-access-token", "--resource", defaultKustoResource, "--output", "json"}
		if tp.tenant != "" {
			args = append(args, "--tenant", tp.tenant)
		}
		cmd := exec.CommandContext(ctx, "az", args...)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			msg := strings.TrimSpace(stderr.String())
			if msg == "" {
				msg = err.Error()
			}
			return "", fmt.Errorf("az account get-access-token failed: %s", msg)
		}
		var parsed struct {
			AccessToken string `json:"accessToken"`
			ExpiresOnTS int64  `json:"expires_on"`
		}
		if err := json.Unmarshal(out, &parsed); err != nil {
			return "", fmt.Errorf("parse az token output: %w", err)
		}
		if parsed.AccessToken == "" {
			return "", errors.New("az returned empty accessToken")
		}
		tp.token = parsed.AccessToken
		tp.expires = time.Now().Add(50 * time.Minute)
		if parsed.ExpiresOnTS > 0 {
			tp.expires = time.Unix(parsed.ExpiresOnTS, 0)
		}
		return tp.token, nil
	}
	return "", fmt.Errorf("unsupported auth mode %q", tp.mode)
}

func toolDefinitions() []map[string]any {
	baseProps := map[string]any{
		"cluster_uri":               map[string]any{"type": "string", "description": "The URI of the Kusto cluster."},
		"database":                  map[string]any{"type": []string{"string", "null"}, "description": "Optional database name. Defaults to the configured database."},
		"client_request_properties": map[string]any{"type": []string{"object", "null"}, "additionalProperties": true, "description": "Optional Kusto client request properties; readonly flags cannot be overridden."},
	}
	with := func(extra map[string]any) map[string]any {
		p := map[string]any{}
		for k, v := range baseProps {
			p[k] = v
		}
		for k, v := range extra {
			p[k] = v
		}
		return p
	}
	tool := func(name, desc string, props map[string]any, required []string) map[string]any {
		return map[string]any{"name": name, "description": desc, "inputSchema": map[string]any{"type": "object", "properties": props, "required": required}}
	}
	return []map[string]any{
		tool("kusto_command", "Execute a Kusto management command (must start with '.') on the specified database.", with(map[string]any{"command": map[string]any{"type": "string"}}), []string{"command", "cluster_uri"}),
		tool("kusto_deeplink_from_query", "Build a deeplink URL that opens a KQL query in Azure Data Explorer or Fabric.", map[string]any{"cluster_uri": map[string]any{"type": "string"}, "database": map[string]any{"type": "string"}, "query": map[string]any{"type": "string"}}, []string{"cluster_uri", "database", "query"}),
		tool("kusto_describe_database", "Retrieve schema information for entities in a database.", with(map[string]any{}), []string{"cluster_uri"}),
		tool("kusto_describe_database_entity", "Retrieve schema information for a specific entity.", with(map[string]any{"entity_name": map[string]any{"type": "string"}, "entity_type": map[string]any{"type": "string"}}), []string{"entity_name", "entity_type", "cluster_uri"}),
		tool("kusto_diagnostics", "Run diagnostic .show commands and return a JSON summary.", with(map[string]any{}), []string{"cluster_uri"}),
		tool("kusto_get_shots", "Retrieve simple KQL examples from a configured shots table; returns [] when none is configured.", with(map[string]any{"prompt": map[string]any{"type": "string"}, "embedding_endpoint": map[string]any{"type": []string{"string", "null"}}, "shots_table_name": map[string]any{"type": []string{"string", "null"}}, "sample_size": map[string]any{"type": []string{"integer", "null"}}}), []string{"prompt", "cluster_uri"}),
		tool("kusto_graph_query", "Execute a graph query by wrapping the provided query with graph('<name>').", with(map[string]any{"graph_name": map[string]any{"type": "string"}, "query": map[string]any{"type": "string"}}), []string{"graph_name", "query", "cluster_uri"}),
		tool("kusto_ingest_inline_into_table", "Ingest inline comma-separated data into a table. Destructive write operation.", with(map[string]any{"table_name": map[string]any{"type": "string"}, "data_comma_separator": map[string]any{"type": "string"}}), []string{"table_name", "data_comma_separator", "cluster_uri"}),
		tool("kusto_known_services", "Retrieve the list of Kusto services known to this CLI.", map[string]any{}, []string{}),
		tool("kusto_list_entities", "List databases, tables, external-tables, materialized-views, functions, or graphs.", with(map[string]any{"entity_type": map[string]any{"type": "string"}}), []string{"cluster_uri", "entity_type"}),
		tool("kusto_query", "Execute a KQL query. For management commands starting with '.', use kusto_command.", with(map[string]any{"query": map[string]any{"type": "string"}}), []string{"query", "cluster_uri"}),
		tool("kusto_sample_entity", "Retrieve a data sample from an entity.", with(map[string]any{"entity_name": map[string]any{"type": "string"}, "entity_type": map[string]any{"type": "string"}, "sample_size": map[string]any{"type": []string{"integer", "null"}}}), []string{"entity_name", "entity_type", "cluster_uri"}),
		tool("kusto_show_queryplan", "Retrieve the query execution plan without running the query.", with(map[string]any{"query": map[string]any{"type": "string"}}), []string{"query", "cluster_uri"}),
	}
}

func isSafeManagementCommand(c string) bool {
	stmt := strings.ToLower(firstStatement(c))
	return strings.HasPrefix(stmt, ".show")
}

func validateQuery(q string) error {
	if strings.TrimSpace(q) == "" {
		return errors.New("query is required")
	}
	if strings.HasPrefix(firstStatement(q), ".") {
		return errors.New("kusto_query is for KQL queries; use kusto_command for management commands")
	}
	return nil
}
func validateCommand(c string) error {
	if strings.TrimSpace(c) == "" {
		return errors.New("command is required")
	}
	if !strings.HasPrefix(firstStatement(c), ".") {
		return errors.New("kusto_command requires a management command starting with '.'")
	}
	return nil
}
func firstStatement(text string) string {
	for _, line := range strings.Split(text, "\n") {
		s := strings.TrimSpace(line)
		if s == "" || strings.HasPrefix(s, "//") || strings.HasPrefix(s, "#") || strings.HasPrefix(strings.ToLower(s), "set ") {
			continue
		}
		return s
	}
	return ""
}
func canonicalEntityType(t string) (string, error) {
	s := strings.ToLower(strings.TrimSpace(t))
	switch s {
	case "database", "databases":
		return "database", nil
	case "table", "tables":
		return "table", nil
	case "external table", "external-table", "externaltable", "external":
		return "external-table", nil
	case "materialized view", "materialized-view", "mv":
		return "materialized-view", nil
	case "function", "functions":
		return "function", nil
	case "graph", "graphs", "graph model", "graph-model":
		return "graph", nil
	default:
		return "", fmt.Errorf("unknown entity type %q; supported: database, table, external-table, materialized-view, function, graph", t)
	}
}
func listEntitiesCommand(entityType, db string) (string, string, error) {
	et, err := canonicalEntityType(entityType)
	if err != nil {
		return "", "", err
	}
	switch et {
	case "database":
		return ".show databases | project DatabaseName, DatabaseAccessMode, PrettyName, DatabaseId", defaultKustoDB, nil
	case "table":
		return ".show tables | project-away DatabaseName", db, nil
	case "external-table":
		return ".show external tables", db, nil
	case "materialized-view":
		return ".show materialized-views", db, nil
	case "function":
		return ".show functions", db, nil
	case "graph":
		return ".show graph_models | project-away DatabaseName", db, nil
	}
	return "", db, nil
}
func describeEntityCommand(entityType, name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", errors.New("entity_name is required")
	}
	et, err := canonicalEntityType(entityType)
	if err != nil {
		return "", err
	}
	e := kqlEscapeEntityName(name)
	switch et {
	case "table":
		return ".show table " + e + " cslschema", nil
	case "external-table":
		return ".show external table " + e + " cslschema", nil
	case "function":
		return ".show function " + e, nil
	case "materialized-view":
		return ".show materialized-view " + e + " | project Name, SourceTable, Query, LastRun, LastRunResult, IsHealthy, IsEnabled, DocString", nil
	case "graph":
		return ".show graph_model " + e + " details | project Name, Model", nil
	default:
		return "", fmt.Errorf("describe not supported for entity type %q", et)
	}
}
func sampleEntityQuery(entityType, name string, n int) (string, error) {
	et, err := canonicalEntityType(entityType)
	if err != nil {
		return "", err
	}
	e := kqlEscapeEntityName(name)
	if et == "table" || et == "materialized-view" || et == "external-table" || et == "function" {
		return fmt.Sprintf("%s | sample %d", e, n), nil
	}
	if et == "graph" {
		g := kqlEscapeString(name)
		node := max(1, n/2)
		edge := max(1, n-node)
		return fmt.Sprintf("let NodeSample = graph('%s') | graph-to-table nodes | take %d | project PackedEntity=pack_all(), EntityType='Node';\nlet EdgeSample = graph('%s') | graph-to-table edges | take %d | project PackedEntity=pack_all(), EntityType='Edge';\nNodeSample | union EdgeSample", g, node, g, edge), nil
	}
	return "", fmt.Errorf("sampling not supported for entity type %q", et)
}
func diagnosticsCommands() map[string]string {
	return map[string]string{"capacity": ".show capacity | project Resource, Total, Consumed, Remaining", "cluster": ".show cluster", "principal_roles": ".show principal roles | project Scope, Role", "diagnostics": ".show diagnostics", "workload_groups": ".show workload_groups", "rowstores": ".show rowstores", "ingestion_failures": ".show ingestion failures | where FailedOn > ago(1d)"}
}

func buildDeeplink(clusterURI, database, query string) string {
	u, _ := url.Parse(strings.TrimRight(clusterURI, "/"))
	host := u.Host
	encoded := gzipBase64(query)
	if strings.Contains(host, "kusto.data.microsoft.com") || strings.Contains(host, "fabric") {
		v := url.Values{}
		v.Set("experience", "fabric-developer")
		v.Set("cluster", clusterURI)
		v.Set("database", database)
		v.Set("query", encoded)
		return "https://fabric.microsoft.com/groups/me/queryworkbenches/querydeeplink?" + v.Encode()
	}
	return "https://dataexplorer.azure.com/clusters/" + url.PathEscape(host) + "/databases/" + url.PathEscape(database) + "?query=" + url.QueryEscape(encoded)
}
func gzipBase64(s string) string {
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	_, _ = zw.Write([]byte(s))
	_ = zw.Close()
	return b64Encode(b.Bytes())
}
func b64Encode(b []byte) string {
	const table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	var out strings.Builder
	for i := 0; i < len(b); i += 3 {
		var n uint32
		rem := len(b) - i
		n = uint32(b[i]) << 16
		if rem > 1 {
			n |= uint32(b[i+1]) << 8
		}
		if rem > 2 {
			n |= uint32(b[i+2])
		}
		out.WriteByte(table[(n>>18)&63])
		out.WriteByte(table[(n>>12)&63])
		if rem > 1 {
			out.WriteByte(table[(n>>6)&63])
		} else {
			out.WriteByte('=')
		}
		if rem > 2 {
			out.WriteByte(table[n&63])
		} else {
			out.WriteByte('=')
		}
	}
	return out.String()
}
func kqlEscapeString(v string) string  { return strings.ReplaceAll(v, "'", "''") }
func kqlStringLiteral(v string) string { return "'" + kqlEscapeString(v) + "'" }
func kqlEscapeEntityName(name string) string {
	n := strings.TrimSpace(name)
	if (strings.HasPrefix(n, "['") && strings.HasSuffix(n, "']")) || (strings.HasPrefix(n, "[\"") && strings.HasSuffix(n, "\"]")) {
		return n
	}
	return "['" + kqlEscapeString(n) + "']"
}
func rowsToDicts(resp kustoResponse) []map[string]any {
	out := make([]map[string]any, 0, len(resp.Data.Rows))
	for _, row := range resp.Data.Rows {
		m := map[string]any{}
		for i, c := range resp.Data.Columns {
			if i < len(row) {
				m[c.ColumnName] = row[i]
			}
		}
		out = append(out, m)
	}
	return out
}
func (cfg config) hasConfiguredDatabase() bool {
	if cfg.databaseConfigured {
		return true
	}
	database := strings.TrimSpace(cfg.database)
	return database != "" && database != defaultKustoDB
}

func (svc KustoServiceConfig) targetAliases() []string {
	aliases := []string{}
	aliases = appendUniqueString(aliases, strings.TrimSpace(svc.Alias))
	aliases = appendUniqueString(aliases, strings.TrimSpace(svc.Name))
	for _, alias := range svc.Aliases {
		aliases = appendUniqueString(aliases, strings.TrimSpace(alias))
	}
	return aliases
}

func (svc KustoServiceConfig) matchesTargetAlias(alias string) bool {
	alias = strings.TrimSpace(alias)
	for _, candidate := range svc.targetAliases() {
		if candidate == alias {
			return true
		}
	}
	return false
}

func (s *server) askTargetCatalog() []askTargetCandidate {
	targets := []askTargetCandidate{}
	indexes := map[string]int{}
	for _, svc := range s.knownServices {
		clusterURI := strings.TrimRight(strings.TrimSpace(svc.ServiceURI), "/")
		database := strings.TrimSpace(svc.DefaultDatabase)
		if clusterURI == "" || database == "" {
			continue
		}
		key := targetKey(clusterURI, database)
		if idx, ok := indexes[key]; ok {
			for _, alias := range svc.targetAliases() {
				targets[idx].Aliases = appendUniqueString(targets[idx].Aliases, alias)
			}
			if targets[idx].Description == "" {
				targets[idx].Description = strings.TrimSpace(svc.Description)
			}
			continue
		}
		indexes[key] = len(targets)
		targets = append(targets, askTargetCandidate{
			ClusterURI:  clusterURI,
			Database:    database,
			Aliases:     svc.targetAliases(),
			Description: strings.TrimSpace(svc.Description),
		})
	}
	return targets
}

func (s *server) targetsForServiceURI(clusterURI string) []queryDraftTarget {
	clusterURI = strings.TrimRight(strings.TrimSpace(clusterURI), "/")
	matches := []queryDraftTarget{}
	for _, target := range s.askTargetCatalog() {
		if normalizeServiceURI(target.ClusterURI) == normalizeServiceURI(clusterURI) {
			matches = appendUniqueQueryDraftTarget(matches, queryDraftTarget{ClusterURI: target.ClusterURI, Database: target.Database})
		}
	}
	return matches
}

func (s *server) configuredServiceURIs() []string {
	services := []string{}
	for _, svc := range s.knownServices {
		clusterURI := strings.TrimRight(strings.TrimSpace(svc.ServiceURI), "/")
		if clusterURI == "" {
			continue
		}
		if !containsNormalizedServiceURI(services, clusterURI) {
			services = append(services, clusterURI)
		}
	}
	return services
}

func (s *server) allTargetAliases() []string {
	aliases := []string{}
	for _, svc := range s.knownServices {
		for _, alias := range svc.targetAliases() {
			aliases = appendUniqueString(aliases, alias)
		}
	}
	return aliases
}

func validateQueryDraftTarget(target queryDraftTarget) (queryDraftTarget, error) {
	target.ClusterURI = strings.TrimRight(strings.TrimSpace(target.ClusterURI), "/")
	target.Database = strings.TrimSpace(target.Database)
	if target.ClusterURI == "" {
		return queryDraftTarget{}, errors.New("ask requires a Target service URI; provide --service-uri or select --target")
	}
	if _, err := url.ParseRequestURI(target.ClusterURI); err != nil {
		return queryDraftTarget{}, fmt.Errorf("service-uri is not a valid URL: %w", err)
	}
	if target.Database == "" {
		return queryDraftTarget{}, errors.New("ask requires a Target database; provide --database or select --target")
	}
	return target, nil
}

func appendUniqueQueryDraftTarget(targets []queryDraftTarget, target queryDraftTarget) []queryDraftTarget {
	key := targetKey(target.ClusterURI, target.Database)
	for _, existing := range targets {
		if targetKey(existing.ClusterURI, existing.Database) == key {
			return targets
		}
	}
	return append(targets, target)
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func containsNormalizedServiceURI(values []string, value string) bool {
	needle := normalizeServiceURI(value)
	for _, existing := range values {
		if normalizeServiceURI(existing) == needle {
			return true
		}
	}
	return false
}

func targetKey(clusterURI, database string) string {
	return normalizeServiceURI(clusterURI) + "\x00" + strings.TrimSpace(database)
}

func formatAskTargets(targets []askTargetCandidate) string {
	if len(targets) == 0 {
		return "  - (none)"
	}
	lines := make([]string, 0, len(targets))
	for _, target := range targets {
		label := strings.Join(target.Aliases, ", ")
		if label == "" {
			lines = append(lines, fmt.Sprintf("  - %s / %s", target.ClusterURI, target.Database))
			continue
		}
		lines = append(lines, fmt.Sprintf("  - %s: %s / %s", label, target.ClusterURI, target.Database))
	}
	return strings.Join(lines, "\n")
}

func formatQueryDraftTargets(targets []queryDraftTarget) string {
	if len(targets) == 0 {
		return "  - (none)"
	}
	lines := make([]string, 0, len(targets))
	for _, target := range targets {
		lines = append(lines, fmt.Sprintf("  - %s / %s", strings.TrimRight(strings.TrimSpace(target.ClusterURI), "/"), strings.TrimSpace(target.Database)))
	}
	return strings.Join(lines, "\n")
}

func formatConfiguredServices(services []string) string {
	if len(services) == 0 {
		return "  - (none)"
	}
	lines := make([]string, 0, len(services))
	for _, service := range services {
		lines = append(lines, "  - "+strings.TrimRight(strings.TrimSpace(service), "/"))
	}
	return strings.Join(lines, "\n")
}

func (s *server) defaultClusterURI() string {
	if s.defaultSvc != nil {
		return s.defaultSvc.ServiceURI
	}
	return ""
}
func (s *server) defaultDatabaseFor(clusterURI, provided string) string {
	if strings.TrimSpace(provided) != "" {
		return strings.TrimSpace(provided)
	}
	key := normalizeServiceURI(clusterURI)
	for _, svc := range s.knownServices {
		if normalizeServiceURI(svc.ServiceURI) != key {
			continue
		}
		if svc.DefaultDatabase != "" {
			return svc.DefaultDatabase
		}
		break
	}
	if s.cfg.database != "" {
		return s.cfg.database
	}
	return defaultKustoDB
}
func normalizeServiceURI(u string) string {
	return strings.ToLower(strings.TrimRight(strings.TrimSpace(u), "/"))
}
func decodeArgs(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return dec.Decode(v)
}
func marshalText(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
func toolResult(text string, isErr bool) map[string]any {
	return map[string]any{"content": []map[string]string{{"type": "text", "text": text}}, "isError": isErr}
}
func writeRPCResult(w *bufio.Writer, id json.RawMessage, result any) {
	obj := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(id), "result": result}
	b, _ := json.Marshal(obj)
	_, _ = w.Write(b)
	_ = w.WriteByte('\n')
	_ = w.Flush()
}
func writeRPCError(w *bufio.Writer, id json.RawMessage, code int, message, data string) {
	if len(id) == 0 {
		id = []byte("null")
	}
	obj := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(id), "error": map[string]any{"code": code, "message": message, "data": data}}
	b, _ := json.Marshal(obj)
	_, _ = w.Write(b)
	_ = w.WriteByte('\n')
	_ = w.Flush()
}
func writeToolError(w *bufio.Writer, id json.RawMessage, err error) {
	writeRPCError(w, id, -32603, "Internal error", err.Error())
}
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
func randomID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b[:])
}
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
func summarize(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
func debugf(enabled bool, format string, args ...any) {
	if enabled {
		log.Printf(format, args...)
	}
}
