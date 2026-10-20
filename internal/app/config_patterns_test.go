package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// withPattern resolves p against cfg (as loadConfig does) and appends it to
// cfg.TopicPatterns so validateConfig can be called directly.
func withPattern(cfg *Config, p TopicPattern) {
	p.resolve(cfg)
	cfg.TopicPatterns = append(cfg.TopicPatterns, p)
}

func validPattern() TopicPattern {
	return TopicPattern{
		Pattern:           `prod\.(?P<d>[a-z]+)\.(?P<e>[a-z_]+)`,
		TopicTableMapping: TopicTableMapping{Table: "analytics.${d}_${e}"},
	}
}

func TestValidateConfigTopicPatterns(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(cfg *Config)
		want   string // empty means the config must validate
	}{
		{
			name:   "valid pattern",
			mutate: func(cfg *Config) { withPattern(cfg, validPattern()) },
		},
		{
			name: "topic key inside a pattern entry",
			mutate: func(cfg *Config) {
				p := validPattern()
				p.Topic = "orders"
				withPattern(cfg, p)
			},
			want: "topic_patterns[0]: topic is not allowed here",
		},
		{
			name: "invalid regex",
			mutate: func(cfg *Config) {
				p := validPattern()
				p.Pattern = `prod\.(`
				withPattern(cfg, p)
			},
			want: "topic_patterns[0]: pattern",
		},
		{
			name: "template references unknown group",
			mutate: func(cfg *Config) {
				p := validPattern()
				p.Table = "analytics.${nope}"
				withPattern(cfg, p)
			},
			want: `topic_patterns[0]: table template "analytics.${nope}" references group "nope"`,
		},
		{
			name: "duplicate pattern",
			mutate: func(cfg *Config) {
				withPattern(cfg, validPattern())
				withPattern(cfg, validPattern())
			},
			want: "topic_patterns[1]: duplicate pattern",
		},
		{
			name: "invalid format on the template",
			mutate: func(cfg *Config) {
				p := validPattern()
				p.Format = "xml"
				withPattern(cfg, p)
			},
			want: "topic_patterns[0]: format must be one of avro, json, or string",
		},
		{
			name: "string format without a value column",
			mutate: func(cfg *Config) {
				cfg.StringValueColumn = ""
				p := validPattern()
				p.Format = "string"
				withPattern(cfg, p)
			},
			want: "topic_patterns[0]: string_value_column is required",
		},
		{
			name: "batch_size override of zero",
			mutate: func(cfg *Config) {
				p := validPattern()
				p.BatchSize = intPtr(0)
				withPattern(cfg, p)
			},
			want: "topic_patterns[0]: batch_size must be at least 1",
		},
		{
			name: "tombstones row mode on a pattern",
			mutate: func(cfg *Config) {
				p := validPattern()
				p.KafkaMetadata = &KafkaMetadataMapping{Offset: "__offset"}
				p.Tombstones = &TombstoneConfig{Mode: TombstoneModeRow, KeyColumn: "id"}
				withPattern(cfg, p)
			},
		},
		{
			name: "tombstones row mode on a pattern without the offset column",
			mutate: func(cfg *Config) {
				p := validPattern()
				p.Tombstones = &TombstoneConfig{Mode: TombstoneModeRow, KeyColumn: "id"}
				withPattern(cfg, p)
			},
			want: "topic_patterns[0]: tombstones.mode row requires kafka_metadata.offset",
		},
		{
			name: "patterns alone satisfy the mapping requirement",
			mutate: func(cfg *Config) {
				cfg.TopicTables = nil
				withPattern(cfg, validPattern())
			},
		},
		{
			name:   "neither tables nor patterns",
			mutate: func(cfg *Config) { cfg.TopicTables = nil },
			want:   "at least one topic_tables mapping or topic_patterns entry is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := validateConfig(&cfg)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Expected config to validate, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Expected error containing %q, got nil", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestValidateConfigAvroPatternRequiresSchemaRegistry(t *testing.T) {
	cfg := validConfig()
	cfg.TopicTables = nil
	cfg.SchemaRegistry = ""
	p := validPattern()
	p.Format = "avro"
	withPattern(&cfg, p)
	if err := validateConfig(&cfg); err == nil || !strings.Contains(err.Error(), "schema_registry is required") {
		t.Fatalf("Expected schema_registry to be required for an avro pattern, got %v", err)
	}

	cfg = validConfig()
	cfg.TopicTables = nil
	cfg.SchemaRegistry = ""
	p = validPattern()
	p.Format = "json"
	withPattern(&cfg, p)
	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("Expected a json-only pattern config to validate without a registry, got %v", err)
	}
}

func TestValidateConfigDiscoverySettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(cfg *Config)
		want   string
	}{
		{"negative interval", func(cfg *Config) { cfg.TopicDiscovery.IntervalS = intPtr(-1) }, "topic_discovery.interval_s must be >= 0"},
		{"zero max_topics", func(cfg *Config) { cfg.TopicDiscovery.MaxTopics = 0 }, "topic_discovery.max_topics must be at least 1"},
		{"zero timeout", func(cfg *Config) { cfg.TopicDiscovery.TimeoutMs = 0 }, "topic_discovery.timeout_ms must be > 0"},
		{"negative readiness grace", func(cfg *Config) { cfg.ReadinessGraceS = intPtr(-5) }, "readiness_grace_s must be >= 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := validateConfig(&cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %v", tt.want, err)
			}
		})
	}

	// Zero interval and zero grace are valid: they disable the loop and the grace.
	cfg := validConfig()
	cfg.TopicDiscovery.IntervalS = intPtr(0)
	cfg.ReadinessGraceS = intPtr(0)
	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("Expected zero interval and grace to validate, got %v", err)
	}
}

func TestApplyDefaultsDiscovery(t *testing.T) {
	cfg := &Config{}
	applyDefaults(cfg)
	if cfg.ReadinessGraceS == nil || *cfg.ReadinessGraceS != 30 {
		t.Errorf("readiness_grace_s default = %v, want 30", cfg.ReadinessGraceS)
	}
	if cfg.TopicDiscovery.IntervalS == nil || *cfg.TopicDiscovery.IntervalS != 60 {
		t.Errorf("topic_discovery.interval_s default = %v, want 60", cfg.TopicDiscovery.IntervalS)
	}
	if cfg.TopicDiscovery.MaxTopics != 200 {
		t.Errorf("topic_discovery.max_topics default = %d, want 200", cfg.TopicDiscovery.MaxTopics)
	}
	if cfg.TopicDiscovery.TimeoutMs != 5000 {
		t.Errorf("topic_discovery.timeout_ms default = %d, want 5000", cfg.TopicDiscovery.TimeoutMs)
	}

	// Explicit values, including zero for the pointer fields, are kept.
	cfg = &Config{ReadinessGraceS: intPtr(0), TopicDiscovery: TopicDiscoveryConfig{IntervalS: intPtr(0), MaxTopics: 7, TimeoutMs: 100}}
	applyDefaults(cfg)
	if *cfg.ReadinessGraceS != 0 || *cfg.TopicDiscovery.IntervalS != 0 || cfg.TopicDiscovery.MaxTopics != 7 || cfg.TopicDiscovery.TimeoutMs != 100 {
		t.Errorf("Explicit discovery settings were overridden: %+v grace=%d", cfg.TopicDiscovery, *cfg.ReadinessGraceS)
	}
}

func TestAnyTopicUsesFormatIncludesPatterns(t *testing.T) {
	cfg := &Config{
		TopicTables:   []TopicTableMapping{{Topic: "orders", Format: "json"}},
		TopicPatterns: []TopicPattern{{Pattern: `prod\..*`, TopicTableMapping: TopicTableMapping{Format: "avro"}}},
	}
	if !anyTopicUsesFormat(cfg, "avro") {
		t.Fatal("Expected a pattern's format to count")
	}
	if !anyTopicUsesFormat(cfg, "json") {
		t.Fatal("Expected an explicit mapping's format to count")
	}
	if anyTopicUsesFormat(cfg, "string") {
		t.Fatal("Expected an unused format not to count")
	}
}

func TestConfigLogFieldsIncludesDiscovery(t *testing.T) {
	cfg := validConfig()
	withPattern(&cfg, validPattern())
	fields := configLogFields(&cfg)
	byKey := make(map[string]interface{}, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		byKey[fields[i].(string)] = fields[i+1]
	}
	for _, key := range []string{"readiness_grace_s", "topic_discovery_interval_s", "topic_discovery_max_topics", "topic_discovery_timeout_ms", "topic_patterns"} {
		if _, ok := byKey[key]; !ok {
			t.Errorf("Expected %q in config log fields", key)
		}
	}
	if byKey["topic_discovery_interval_s"] != 60 || byKey["readiness_grace_s"] != 30 {
		t.Errorf("Unexpected logged values: interval=%v grace=%v", byKey["topic_discovery_interval_s"], byKey["readiness_grace_s"])
	}
}

func TestLoadConfigParsesTopicPatterns(t *testing.T) {
	yamlContent := `
kafka_brokers: "localhost:9092"
clickhouse_dsn: "tcp://localhost:9000"
input_format: json
readiness_grace_s: 10
topic_discovery:
  interval_s: 15
  max_topics: 50
topic_patterns:
  - pattern: "prod\\.(?P<domain>[a-z]+)\\.(?P<entity>[a-z_]+)"
    table: "analytics.${domain}_${entity}"
    table_replace: {".": "_"}
    format: json
    batch_size: 500
    start_at:
      position: latest
  - pattern: "cdc\\.public\\.(.+)"
    table: "raw.${1}"
    format: string
    string_value_column: "payload"
`
	path := filepath.Join(t.TempDir(), "kahouse.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if len(cfg.TopicTables) != 0 {
		t.Fatalf("Expected no explicit mappings, got %d", len(cfg.TopicTables))
	}
	if len(cfg.TopicPatterns) != 2 {
		t.Fatalf("Expected 2 patterns, got %d", len(cfg.TopicPatterns))
	}
	if *cfg.ReadinessGraceS != 10 || *cfg.TopicDiscovery.IntervalS != 15 || cfg.TopicDiscovery.MaxTopics != 50 || cfg.TopicDiscovery.TimeoutMs != 5000 {
		t.Fatalf("Unexpected discovery settings: %+v grace=%d", cfg.TopicDiscovery, *cfg.ReadinessGraceS)
	}

	p := cfg.TopicPatterns[0]
	if p.Table != "analytics.${domain}_${entity}" || p.TableReplace["."] != "_" || p.Format != "json" || *p.BatchSize != 500 {
		t.Fatalf("Unexpected first pattern: table=%q replace=%v format=%q batch_size=%d", p.Table, p.TableReplace, p.Format, *p.BatchSize)
	}
	if p.StartAt == nil || p.StartAt.Position != "latest" {
		t.Fatalf("Expected start_at.position=latest on the template, got %+v", p.StartAt)
	}
	// Inherited defaults land on the template like on an explicit mapping.
	if *p.MaxRetries != 5 || *p.BatchDelayMs != 200 {
		t.Fatalf("Expected global defaults on the template, got max_retries=%d batch_delay_ms=%d", *p.MaxRetries, *p.BatchDelayMs)
	}

	m, ok := p.expand("prod.hr.employees")
	if !ok {
		t.Fatal("Expected prod.hr.employees to match the loaded pattern")
	}
	if m.Table != "analytics.hr_employees" || m.Topic != "prod.hr.employees" || *m.BatchSize != 500 || m.SourcePattern != p.Pattern {
		t.Fatalf("Unexpected expansion: %+v", m)
	}

	second := cfg.TopicPatterns[1]
	if second.Format != "string" || second.StringValueColumn != "payload" {
		t.Fatalf("Unexpected second pattern: format=%q column=%q", second.Format, second.StringValueColumn)
	}
	if m, ok := second.expand("cdc.public.invoices"); !ok || m.Table != "raw.invoices" {
		t.Fatalf("Unexpected expansion for second pattern: %+v ok=%v", m, ok)
	}
}

func TestLoadConfigRejectsTopicInsidePattern(t *testing.T) {
	yamlContent := `
kafka_brokers: "localhost:9092"
clickhouse_dsn: "tcp://localhost:9000"
input_format: json
topic_patterns:
  - pattern: "prod\\..*"
    topic: "oops"
    table: "analytics.${0}"
`
	path := filepath.Join(t.TempDir(), "kahouse.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "topic_patterns[0]: topic is not allowed here") {
		t.Fatalf("Expected the topic key to be rejected, got %v", err)
	}
}
