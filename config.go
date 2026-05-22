package ruler

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
	"gopkg.in/yaml.v3"
)

// Config represents a Prometheus rule file (groups + rules).
type Config struct {
	Groups []Group `yaml:"groups"`
}

// ParseConfig parses the YAML config and validates it.
func ParseConfig(b []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}

	for i := range cfg.Groups {
		if err := cfg.Groups[i].Validate(); err != nil {
			return nil, fmt.Errorf("group %q: %w", cfg.Groups[i].Name, err)
		}
	}

	return &cfg, nil
}

// ParseConfigFile loads and parses a rule file, tagging groups with the file path.
func ParseConfigFile(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg, err := ParseConfig(b)
	if err != nil {
		return nil, err
	}

	for i := range cfg.Groups {
		cfg.Groups[i].File = path
	}

	return cfg, nil
}

// Header is a Key: Value pair parsed from YAML.
type Header struct {
	Key   string
	Value string
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (h *Header) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	if s == "" {
		return nil
	}

	n := strings.IndexByte(s, ':')
	if n < 0 {
		return fmt.Errorf(`missing ':' in header %q; expecting "key: value" format`, s)
	}

	h.Key = strings.TrimSpace(s[:n])
	h.Value = strings.TrimSpace(s[n+1:])
	if h.Key == "" {
		return fmt.Errorf("header key cannot be empty")
	}

	return nil
}

// Params holds query params for rule evaluation.
type Params map[string][]string

// UnmarshalYAML supports both scalar and list values.
func (p *Params) UnmarshalYAML(unmarshal func(any) error) error {
	var raw map[string]any
	if err := unmarshal(&raw); err != nil {
		return err
	}

	out := make(Params, len(raw))
	for k, v := range raw {
		switch tv := v.(type) {
		case string:
			out[k] = []string{tv}
		case []any:
			vals := make([]string, 0, len(tv))
			for _, item := range tv {
				s, ok := item.(string)
				if !ok {
					return fmt.Errorf("param %q must be a string or list of strings", k)
				}
				vals = append(vals, s)
			}
			out[k] = vals
		default:
			return fmt.Errorf("param %q must be a string or list of strings", k)
		}
	}
	
	*p = out
	return nil
}

// Values converts Params to url.Values.
func (p Params) Values() url.Values {
	if p == nil {
		return nil
	}

	uv := make(url.Values, len(p))
	for k, v := range p {
		uv[k] = append([]string(nil), v...)
	}

	return uv
}

// Group describes a rule group with vmalert-style extras.
type Group struct {
	Type          string             `yaml:"type,omitempty"`
	File          string             `yaml:"-"`
	Name          string             `yaml:"name"`
	Interval      *model.Duration    `yaml:"interval,omitempty"`
	EvalOffset    *model.Duration    `yaml:"eval_offset,omitempty"`
	EvalDelay     *model.Duration    `yaml:"eval_delay,omitempty"`
	Limit         *int               `yaml:"limit,omitempty"`
	Rules         []Rule             `yaml:"rules"`
	Concurrency   int                `yaml:"concurrency,omitempty"`
	Labels        map[string]string  `yaml:"labels,omitempty"`
	Params        Params             `yaml:"params,omitempty"`
	Headers       []Header           `yaml:"headers,omitempty"`
	EvalAlignment *bool              `yaml:"eval_alignment,omitempty"`
	Replay        *GroupReplayConfig `yaml:"replay,omitempty"`
	Debug         bool               `yaml:"debug,omitempty"`

	Checksum string         `yaml:"-"`
	XXX      map[string]any `yaml:",inline"`
}

// UnmarshalYAML calculates the group checksum after parsing.
func (g *Group) UnmarshalYAML(unmarshal func(any) error) error {
	type group Group
	if err := unmarshal((*group)(g)); err != nil {
		return err
	}

	if g.Type == "" {
		g.Type = "prometheus"
	}

	b, err := yaml.Marshal(g)
	if err != nil {
		return fmt.Errorf("failed to marshal group configuration for checksum: %w", err)
	}

	sum := sha256.Sum256(b)
	g.Checksum = hex.EncodeToString(sum[:])

	return nil
}

// Validate checks configuration errors for group and internal rules.
func (g *Group) Validate() error {
	if g.Name == "" {
		return fmt.Errorf("group name must be set")
	}

	if g.Type != "prometheus" {
		return fmt.Errorf("unsupported datasource type %q (only prometheus is supported)", g.Type)
	}

	if g.Interval != nil && time.Duration(*g.Interval) < 0 {
		return fmt.Errorf("interval shouldn't be lower than 0")
	}

	if g.EvalOffset != nil && time.Duration(*g.EvalOffset) < 0 {
		return fmt.Errorf("eval_offset shouldn't be lower than 0")
	}

	if g.Interval != nil && g.EvalOffset != nil && time.Duration(*g.EvalOffset) > time.Duration(*g.Interval) {
		return fmt.Errorf("eval_offset should be smaller than interval; now eval_offset: %v, interval: %v", time.Duration(*g.EvalOffset), time.Duration(*g.Interval))
	}

	if g.EvalOffset != nil && g.EvalDelay != nil {
		return fmt.Errorf("eval_offset cannot be used with eval_delay")
	}

	if g.Limit != nil && *g.Limit < 0 {
		return fmt.Errorf("invalid limit %d, shouldn't be less than 0", *g.Limit)
	}

	if g.Concurrency < 0 {
		return fmt.Errorf("invalid concurrency %d, shouldn't be less than 0", g.Concurrency)
	}

	uniqueRules := map[uint64]struct{}{}
	for i := range g.Rules {
		ruleName := g.Rules[i].Record
		if g.Rules[i].Alert != "" {
			ruleName = g.Rules[i].Alert
		}

		if _, ok := uniqueRules[g.Rules[i].ID]; ok {
			return fmt.Errorf("%q is a duplicate in group", g.Rules[i].String())
		}

		uniqueRules[g.Rules[i].ID] = struct{}{}
		if err := g.Rules[i].Validate(); err != nil {
			return fmt.Errorf("invalid rule %q: %w", ruleName, err)
		}

		if _, err := parser.ParseExpr(g.Rules[i].Expr); err != nil {
			return fmt.Errorf("invalid expression for rule %q: %w", ruleName, err)
		}
	}

	if err := g.Replay.Validate(); err != nil {
		return fmt.Errorf("invalid replay config: %w", err)
	}

	return checkOverflow(g.XXX, fmt.Sprintf("group %q", g.Name))
}

// Rule describes a single rule entry from a Prometheus rule file.
type Rule struct {
	ID                 uint64            `yaml:"-"`
	Record             string            `yaml:"record,omitempty"`
	Alert              string            `yaml:"alert,omitempty"`
	Expr               string            `yaml:"expr"`
	For                *model.Duration   `yaml:"for,omitempty"`
	KeepFiringFor      *model.Duration   `yaml:"keep_firing_for,omitempty"`
	Labels             map[string]string `yaml:"labels,omitempty"`
	Annotations        map[string]string `yaml:"annotations,omitempty"`
	Debug              *bool             `yaml:"debug,omitempty"`
	UpdateEntriesLimit *int              `yaml:"update_entries_limit,omitempty"`

	XXX map[string]any `yaml:",inline"`
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (r *Rule) UnmarshalYAML(unmarshal func(any) error) error {
	type rule Rule
	if err := unmarshal((*rule)(r)); err != nil {
		return err
	}

	r.ID = HashRule(*r)

	return nil
}

// Name returns Rule name according to its type.
func (r *Rule) Name() string {
	if r.Record != "" {
		return r.Record
	}

	return r.Alert
}

// String implements Stringer interface.
func (r *Rule) String() string {
	ruleType := "recording"
	if r.Alert != "" {
		ruleType = "alerting"
	}

	b := strings.Builder{}
	b.WriteString(fmt.Sprintf("%s rule %q", ruleType, r.Name()))
	b.WriteString(fmt.Sprintf("; expr: %q", r.Expr))

	kv := sortMap(r.Labels)
	for i := range kv {
		if i == 0 {
			b.WriteString("; labels:")
		}
		b.WriteString(" ")
		b.WriteString(kv[i].key)
		b.WriteString("=")
		b.WriteString(kv[i].value)
		if i < len(kv)-1 {
			b.WriteString(",")
		}
	}

	return b.String()
}

// Validate checks for rule configuration errors.
func (r *Rule) Validate() error {
	if (r.Record == "" && r.Alert == "") || (r.Record != "" && r.Alert != "") {
		return fmt.Errorf("either `record` or `alert` must be set")
	}

	if r.Alert != "" {
		return fmt.Errorf("alerting rules are not supported")
	}

	if r.Expr == "" {
		return fmt.Errorf("expression can't be empty")
	}

	if !model.IsValidMetricName(model.LabelValue(r.Record)) {
		return fmt.Errorf("invalid record name %q", r.Record)
	}

	for k := range r.Labels {
		if !model.LabelName(k).IsValid() {
			return fmt.Errorf("invalid label name %q", k)
		}
	}

	if r.For != nil || r.KeepFiringFor != nil || len(r.Annotations) > 0 {
		return fmt.Errorf("alerting-specific fields are not supported in recording rules")
	}

	return checkOverflow(r.XXX, fmt.Sprintf("rule %q", r.Name()))
}

// HashRule returns a stable hash of the rule definition.
func HashRule(r Rule) uint64 {
	h := fnv64a{}
	h.WriteString(r.Expr)

	if r.Record != "" {
		h.WriteString("recording")
		h.WriteString(r.Record)
	} else {
		h.WriteString("alerting")
		h.WriteString(r.Alert)
	}

	kv := sortMap(r.Labels)
	for _, i := range kv {
		h.WriteString(i.key)
		h.WriteString(i.value)
		h.WriteString("\xff")
	}

	return h.Sum64()
}

type kvPair struct {
	key   string
	value string
}

func sortMap(m map[string]string) []kvPair {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	out := make([]kvPair, 0, len(keys))
	for _, k := range keys {
		out = append(out, kvPair{key: k, value: m[k]})
	}

	return out
}

func checkOverflow(m map[string]any, name string) error {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return fmt.Errorf("%s: unknown fields: %s", name, strings.Join(keys, ", "))
}

// fnv64a is a small helper to avoid allocations from hash.Hash.
type fnv64a struct {
	sum uint64
}

func (h *fnv64a) WriteString(s string) {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	if h.sum == 0 {
		h.sum = offset64
	}
	
	for i := 0; i < len(s); i++ {
		h.sum ^= uint64(s[i])
		h.sum *= prime64
	}
}

func (h *fnv64a) Sum64() uint64 {
	return h.sum
}
