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
)

type config struct {
	serviceURI    string
	database      string
	knownServices string
	tokenEnv      string
	authMode      string
	tenant        string
	userAgent     string
	timeout       time.Duration
	output        string
	allowWrite    bool
	dryRun        bool
	noInput       bool
	force         bool
	debug         bool
	printVersion  bool
	args          []string
}

type server struct {
	cfg           config
	hc            *http.Client
	auth          *tokenProvider
	knownServices []KustoServiceConfig
	defaultSvc    *KustoServiceConfig
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
	ServiceURI      string `json:"service_uri"`
	Service         string `json:"service,omitempty"`
	DefaultDatabase string `json:"default_database,omitempty"`
	Description     string `json:"description,omitempty"`
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
	flag.StringVar(&cfg.serviceURI, "service-uri", firstNonEmpty(os.Getenv("KUSTO_SERVICE_URI"), ""), "default Kusto cluster URI")
	flag.StringVar(&cfg.serviceURI, "service-url", firstNonEmpty(os.Getenv("KUSTO_SERVICE_URI"), ""), "alias for --service-uri")
	flag.StringVar(&cfg.database, "database", firstNonEmpty(os.Getenv("KUSTO_SERVICE_DEFAULT_DB"), defaultKustoDB), "default database")
	flag.StringVar(&cfg.knownServices, "known-services", os.Getenv("KUSTO_KNOWN_SERVICES"), "JSON array of known services")
	flag.StringVar(&cfg.tokenEnv, "token-env", "KUSTO_ACCESS_TOKEN", "environment variable containing a Kusto bearer token")
	flag.StringVar(&cfg.authMode, "auth", "auto", "auth mode: auto, env, azcli, none")
	flag.StringVar(&cfg.tenant, "tenant", "", "optional Azure tenant id for az CLI token acquisition")
	flag.StringVar(&cfg.userAgent, "user-agent", "kusto-cli/"+version.Version, "User-Agent sent to Kusto")
	flag.DurationVar(&cfg.timeout, "timeout", 90*time.Second, "Kusto HTTP timeout")
	flag.StringVar(&cfg.output, "output", firstNonEmpty(os.Getenv("KUSTO_OUTPUT"), "json"), "Direct command output: json, table, or tsv")
	flag.StringVar(&cfg.output, "o", firstNonEmpty(os.Getenv("KUSTO_OUTPUT"), "json"), "Alias for --output")
	flag.BoolVar(&cfg.allowWrite, "allow-write", false, "Allow destructive Kusto operations")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "Preview write operations without executing")
	flag.BoolVar(&cfg.noInput, "no-input", false, "Never prompt; fail instead (reserved for scripting consistency)")
	flag.BoolVar(&cfg.force, "force", false, "Skip confirmation prompts (reserved for scripting consistency)")
	flag.BoolVar(&cfg.debug, "debug", false, "write debug logs to stderr")
	flag.BoolVar(&cfg.printVersion, "version", false, "print version and exit")
	flag.Parse()
	cfg.args = flag.Args()
	applyFileConfig(&cfg, visitedFlags())
	return cfg
}

func run(ctx context.Context, cfg config) error {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSHandshakeTimeout = cfg.timeout
	s := &server{
		cfg:  cfg,
		hc:   &http.Client{Timeout: cfg.timeout, Transport: transport},
		auth: &tokenProvider{mode: cfg.authMode, tokenEnv: cfg.tokenEnv, tenant: cfg.tenant, debug: cfg.debug},
	}
	if err := s.loadKnownServices(); err != nil {
		return err
	}
	if len(cfg.args) == 0 || cfg.args[0] == "serve" {
		return s.serveStdio(ctx, os.Stdin, os.Stdout)
	}
	return s.runCommand(ctx, cfg.args)
}

func (s *server) loadKnownServices() error {
	seen := map[string]bool{}
	add := func(svc KustoServiceConfig) {
		if svc.ServiceURI == "" {
			svc.ServiceURI = svc.Service
		}
		svc.ServiceURI = strings.TrimSpace(svc.ServiceURI)
		if svc.ServiceURI == "" {
			return
		}
		if svc.DefaultDatabase == "" {
			svc.DefaultDatabase = s.cfg.database
		}
		key := normalizeServiceURI(svc.ServiceURI)
		if seen[key] {
			return
		}
		seen[key] = true
		s.knownServices = append(s.knownServices, svc)
		if s.defaultSvc == nil {
			copy := svc
			s.defaultSvc = &copy
		}
	}
	if s.cfg.serviceURI != "" {
		add(KustoServiceConfig{ServiceURI: s.cfg.serviceURI, DefaultDatabase: s.cfg.database, Description: "Default"})
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
	}
	if !visited["known-services"] && os.Getenv("KUSTO_KNOWN_SERVICES") == "" && fileCfg["known-services"] != "" {
		cfg.knownServices = fileCfg["known-services"]
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
	commands := "query command databases tables entities services deeplink queryplan diagnostics api auth config completion"
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
	instructions := "Standalone Kusto MCP server — query and explore Azure Data Explorer / Microsoft Fabric Eventhouse (Kusto) clusters."
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
			a.SampleSize = 5
		}
		query := fmt.Sprintf("%s | where * has %s | take %d", kqlEscapeEntityName(a.ShotsTableName), strconv.Quote(a.Prompt), a.SampleSize)
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
		tool("kusto_known_services", "Retrieve the list of Kusto services known to this MCP.", map[string]any{}, []string{}),
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
func kqlEscapeString(v string) string { return strings.ReplaceAll(v, "'", "''") }
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
		if normalizeServiceURI(svc.ServiceURI) == key && svc.DefaultDatabase != "" {
			return svc.DefaultDatabase
		}
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
