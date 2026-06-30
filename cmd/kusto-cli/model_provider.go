package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

const defaultOpenAICompatibleEndpoint = "https://api.openai.com/v1/chat/completions"

type modelProvider interface {
	GenerateQueryDraft(context.Context, queryDraftRequest) (queryDraft, error)
}

type providerBackedQueryDraftAgent struct {
	provider modelProvider
}

type fakeModelProvider struct{}

type openAICompatibleModelProvider struct {
	endpoint string
	model    string
	apiKey   string
	hc       *http.Client
}

type openAIChatCompletionRequest struct {
	Model          string                   `json:"model"`
	Messages       []openAIChatMessage      `json:"messages"`
	ResponseFormat openAIChatResponseFormat `json:"response_format"`
	Temperature    *float64                 `json:"temperature,omitempty"`
}

type openAIChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIChatResponseFormat struct {
	Type       string               `json:"type"`
	JSONSchema openAIChatJSONSchema `json:"json_schema"`
}

type openAIChatJSONSchema struct {
	Name   string         `json:"name"`
	Strict bool           `json:"strict"`
	Schema map[string]any `json:"schema"`
}

type openAIChatCompletionResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
			Refusal string `json:"refusal,omitempty"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type,omitempty"`
	} `json:"error,omitempty"`
}

type modelQueryDraftOutput struct {
	Query                 string                 `json:"query"`
	ClarificationRequired bool                   `json:"clarification_required,omitempty"`
	ClarificationQuestion string                 `json:"clarification_question,omitempty"`
	Assumptions           []string               `json:"assumptions"`
	Warnings              []string               `json:"warnings"`
	ModelSafety           *queryDraftModelSafety `json:"model_safety,omitempty"`
}

func (s *server) queryDraftAgentForAsk() (queryDraftAgent, error) {
	if s.queryDraftAgent != nil {
		return s.queryDraftAgent, nil
	}
	provider, err := newConfiguredModelProvider(s.cfg, s.hc)
	if err != nil {
		return nil, err
	}
	return providerBackedQueryDraftAgent{provider: provider}, nil
}

func newConfiguredModelProvider(cfg config, hc *http.Client) (modelProvider, error) {
	switch normalizeModelProviderName(cfg.modelProvider) {
	case "", "fake":
		return fakeModelProvider{}, nil
	case "openai-compatible", "openai":
		return newOpenAICompatibleModelProvider(cfg, hc)
	default:
		return nil, fmt.Errorf("unknown model-provider %q (expected fake or openai-compatible)", cfg.modelProvider)
	}
}

func normalizeModelProviderName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "_", "-")
	return name
}

func newOpenAICompatibleModelProvider(cfg config, hc *http.Client) (openAICompatibleModelProvider, error) {
	endpoint := strings.TrimSpace(cfg.modelEndpoint)
	if endpoint == "" {
		endpoint = defaultOpenAICompatibleEndpoint
	}
	if _, err := url.ParseRequestURI(endpoint); err != nil {
		return openAICompatibleModelProvider{}, fmt.Errorf("model-endpoint is not a valid URL: %w", err)
	}
	model := strings.TrimSpace(cfg.modelName)
	if model == "" {
		return openAICompatibleModelProvider{}, errors.New("model is required for model-provider openai-compatible; set --model or KUSTO_MODEL")
	}
	apiKeyEnv := strings.TrimSpace(cfg.modelAPIKeyEnv)
	if apiKeyEnv == "" {
		apiKeyEnv = "OPENAI_API_KEY"
	}
	apiKey := strings.TrimSpace(os.Getenv(apiKeyEnv))
	if apiKey == "" {
		return openAICompatibleModelProvider{}, fmt.Errorf("model API key environment variable %s is empty or not set", apiKeyEnv)
	}
	if hc == nil {
		hc = http.DefaultClient
	}
	return openAICompatibleModelProvider{endpoint: endpoint, model: model, apiKey: apiKey, hc: hc}, nil
}

func (a providerBackedQueryDraftAgent) GenerateQueryDraft(ctx context.Context, req queryDraftRequest) (queryDraft, error) {
	provider := a.provider
	if provider == nil {
		provider = fakeModelProvider{}
	}
	return provider.GenerateQueryDraft(ctx, req)
}

func (a providerBackedQueryDraftAgent) RepairQueryDraft(ctx context.Context, req queryDraftRepairRequest) (queryDraft, error) {
	provider := a.provider
	if provider == nil {
		provider = fakeModelProvider{}
	}
	repairer, ok := provider.(queryDraftRepairer)
	if !ok {
		return queryDraft{}, errors.New("configured model provider does not support Repair Passes")
	}
	return repairer.RepairQueryDraft(ctx, req)
}

func (fakeModelProvider) GenerateQueryDraft(_ context.Context, req queryDraftRequest) (queryDraft, error) {
	prompt := strings.TrimSpace(req.Prompt)
	if prompt == "" {
		return queryDraft{}, errors.New("prompt is required")
	}
	return queryDraft{
		Format: "query_draft",
		Target: req.Target,
		Prompt: prompt,
		Query:  "search " + kqlStringLiteral(prompt) + "\n| take 10",
		Assumptions: []string{
			"Generated by the fake Query Draft Agent tracer bullet.",
			"No real model provider was used.",
		},
		Warnings: []string{
			"Generated KQL was not executed.",
			"Review the Query Draft before using an execution gate.",
		},
		SchemaContext:  req.SchemaContext,
		Examples:       req.Examples,
		DataDisclosure: req.DataDisclosure,
	}, nil
}

func (fakeModelProvider) RepairQueryDraft(_ context.Context, req queryDraftRepairRequest) (queryDraft, error) {
	query := strings.TrimSpace(req.PreviousQuery)
	if query == "" {
		query = "search " + kqlStringLiteral(strings.TrimSpace(req.Prompt)) + "\n| take 10"
	}
	if strings.Contains(strings.ToLower(req.ValidationError), "result bound") && !isBoundedQueryDraft(splitKQLStatements(query)) {
		query += "\n| take 10"
	}
	return queryDraft{
		Format: "query_draft",
		Target: req.Target,
		Prompt: req.Prompt,
		Query:  query,
		Assumptions: []string{
			"Generated by the fake Query Draft Agent Repair Pass.",
			"No real model provider was used.",
		},
		Warnings: []string{
			"Repair Pass used validation errors and Schema Context only; review the Query Draft before execution.",
		},
		SchemaContext:  req.SchemaContext,
		Examples:       req.Examples,
		DataDisclosure: req.DataDisclosure,
	}, nil
}

func (p openAICompatibleModelProvider) GenerateQueryDraft(ctx context.Context, req queryDraftRequest) (queryDraft, error) {
	return p.generateStructuredQueryDraft(ctx, openAIQueryDraftMessages(req))
}

func (p openAICompatibleModelProvider) RepairQueryDraft(ctx context.Context, req queryDraftRepairRequest) (queryDraft, error) {
	return p.generateStructuredQueryDraft(ctx, openAIQueryDraftRepairMessages(req))
}

func (p openAICompatibleModelProvider) generateStructuredQueryDraft(ctx context.Context, messages []openAIChatMessage) (queryDraft, error) {
	body, err := json.Marshal(openAIChatCompletionRequest{
		Model:          p.model,
		Messages:       messages,
		ResponseFormat: openAIQueryDraftResponseFormat(),
		Temperature:    float64Ptr(0),
	})
	if err != nil {
		return queryDraft{}, fmt.Errorf("build model provider request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return queryDraft{}, fmt.Errorf("build model provider request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+bearerToken(p.apiKey))

	hc := p.hc
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(httpReq)
	if err != nil {
		return queryDraft{}, fmt.Errorf("model provider request failed: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return queryDraft{}, fmt.Errorf("read model provider response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet := safeResponseSnippet(string(respBody), p.apiKey)
		if snippet == "" {
			return queryDraft{}, fmt.Errorf("model provider returned HTTP status %d", resp.StatusCode)
		}
		return queryDraft{}, fmt.Errorf("model provider returned HTTP status %d: %s", resp.StatusCode, snippet)
	}

	var parsed openAIChatCompletionResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return queryDraft{}, fmt.Errorf("parse model provider response: %w", err)
	}
	if parsed.Error != nil {
		msg := safeResponseSnippet(parsed.Error.Message, p.apiKey)
		if msg == "" {
			msg = "provider returned an error"
		}
		return queryDraft{}, fmt.Errorf("model provider error: %s", msg)
	}
	if len(parsed.Choices) == 0 {
		return queryDraft{}, errors.New("model provider response did not include choices")
	}
	choice := parsed.Choices[0].Message
	if strings.TrimSpace(choice.Refusal) != "" {
		return queryDraft{}, errors.New("model provider refused to produce a Query Draft")
	}
	content := strings.TrimSpace(choice.Content)
	if content == "" {
		return queryDraft{}, errors.New("model provider response did not include structured content")
	}
	draft, err := parseModelQueryDraftOutput(content)
	if err != nil {
		return queryDraft{}, err
	}
	return draft, nil
}

func openAIQueryDraftMessages(req queryDraftRequest) []openAIChatMessage {
	requestJSON, _ := json.MarshalIndent(req, "", "  ")
	return []openAIChatMessage{
		{
			Role: "system",
			Content: strings.Join([]string{
				"You are the Query Draft Agent for kusto-cli.",
				"Return only JSON matching the requested schema.",
				"Generate one read-only KQL query for the provided Target and natural-language prompt.",
				"Use the provided Schema Context; do not invent private cluster, database, or customer details.",
				"Use any provided examples only as read-only KQL shape guidance; adapt table and column names to the Schema Context instead of copying unrelated example identifiers.",
				"If ambiguity blocks a safe table or function choice, set clarification_required true, ask one concise clarification_question, and leave query empty.",
				"If ambiguity is non-blocking, set clarification_required false and record explicit assumptions.",
				"Do not execute queries. The CLI will run independent validation after your advisory model_safety classification.",
			}, " "),
		},
		{
			Role:    "user",
			Content: string(requestJSON),
		},
	}
}

func openAIQueryDraftRepairMessages(req queryDraftRepairRequest) []openAIChatMessage {
	requestJSON, _ := json.MarshalIndent(req, "", "  ")
	return []openAIChatMessage{
		{
			Role: "system",
			Content: strings.Join([]string{
				"You are the Query Draft Agent for kusto-cli performing exactly one Repair Pass.",
				"Return only JSON matching the requested schema.",
				"Repair the previous read-only KQL query using only the Target, original prompt, Schema Context, examples, and validation error.",
				"Use examples only as read-only KQL shape guidance; adapt identifiers to the Schema Context.",
				"Do not execute queries, request sample data, infer from query results, or invent private cluster, database, or customer details.",
				"If the validation error cannot be safely repaired from the Schema Context, set clarification_required true, ask one concise clarification_question, and leave query empty.",
				"The CLI will run independent static and Kusto-side validation after this Repair Pass.",
			}, " "),
		},
		{
			Role:    "user",
			Content: string(requestJSON),
		},
	}
}

func openAIQueryDraftResponseFormat() openAIChatResponseFormat {
	return openAIChatResponseFormat{
		Type: "json_schema",
		JSONSchema: openAIChatJSONSchema{
			Name:   "kusto_query_draft",
			Strict: true,
			Schema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"query":                  map[string]any{"type": "string", "description": "Read-only KQL query text, or empty when clarification_required is true."},
					"clarification_required": map[string]any{"type": "boolean", "description": "True only when safe query drafting is blocked by ambiguity."},
					"clarification_question": map[string]any{"type": "string", "description": "A concise question to unblock drafting, or empty when clarification_required is false."},
					"assumptions": map[string]any{
						"type":  "array",
						"items": map[string]any{"type": "string"},
					},
					"warnings": map[string]any{
						"type":  "array",
						"items": map[string]any{"type": "string"},
					},
					"model_safety": map[string]any{
						"type":                 "object",
						"additionalProperties": false,
						"properties": map[string]any{
							"classification": map[string]any{"type": "string", "enum": []string{"safe", "unsafe", "unknown"}},
							"reason":         map[string]any{"type": "string"},
						},
						"required": []string{"classification", "reason"},
					},
				},
				"required": []string{"query", "clarification_required", "clarification_question", "assumptions", "warnings", "model_safety"},
			},
		},
	}
}

func parseModelQueryDraftOutput(content string) (queryDraft, error) {
	var out modelQueryDraftOutput
	if err := json.Unmarshal([]byte(content), &out); err != nil {
		return queryDraft{}, fmt.Errorf("model provider returned malformed structured output: %w", err)
	}
	out.Query = strings.TrimSpace(out.Query)
	out.ClarificationQuestion = strings.TrimSpace(out.ClarificationQuestion)
	if out.ClarificationRequired && out.ClarificationQuestion == "" {
		return queryDraft{}, errors.New("model provider returned malformed structured output: clarification_question is required when clarification_required is true")
	}
	if out.Query == "" && !out.ClarificationRequired {
		return queryDraft{}, errors.New("model provider returned malformed structured output: query is required")
	}
	if out.Assumptions == nil {
		out.Assumptions = []string{}
	}
	if out.Warnings == nil {
		out.Warnings = []string{}
	}
	if out.ModelSafety != nil && (out.ModelSafety.Classification != "" || out.ModelSafety.Reason != "") {
		out.ModelSafety.Advisory = true
	}
	return queryDraft{
		Query:                 out.Query,
		ClarificationRequired: out.ClarificationRequired,
		ClarificationQuestion: out.ClarificationQuestion,
		Assumptions:           out.Assumptions,
		Warnings:              out.Warnings,
		ModelSafety:           out.ModelSafety,
	}, nil
}

func bearerToken(secret string) string {
	trimmed := strings.TrimSpace(secret)
	if len(trimmed) >= len("bearer ") && strings.EqualFold(trimmed[:len("bearer ")], "bearer ") {
		return strings.TrimSpace(trimmed[len("bearer "):])
	}
	return trimmed
}

func safeResponseSnippet(text string, secrets ...string) string {
	text = strings.TrimSpace(redactSecrets(text, secrets...))
	if len(text) > 500 {
		text = text[:500] + "..."
	}
	return text
}

func redactSecrets(text string, secrets ...string) string {
	redacted := text
	for _, secret := range secrets {
		secret = strings.TrimSpace(secret)
		if secret == "" {
			continue
		}
		redacted = strings.ReplaceAll(redacted, secret, "[REDACTED]")
		redacted = strings.ReplaceAll(redacted, bearerToken(secret), "[REDACTED]")
		redacted = strings.ReplaceAll(redacted, "Bearer "+bearerToken(secret), "Bearer [REDACTED]")
		redacted = strings.ReplaceAll(redacted, "bearer "+bearerToken(secret), "bearer [REDACTED]")
	}
	return redacted
}

func float64Ptr(v float64) *float64 { return &v }
