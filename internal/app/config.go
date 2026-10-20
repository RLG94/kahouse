package app

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.yaml.in/yaml/v3"
)

// TopicTableMapping defines a single topic-to-table mapping with optional per-topic overrides.
// Numeric fields are pointers so that nil ("not set") can be distinguished from an explicit zero.
// A nil field inherits the global default; a non-nil field (including *0) is used as-is.
type TopicTableMapping struct {
	Topic             string                `yaml:"topic"`
	Table             string                `yaml:"table"`
	Format            string                `yaml:"format,omitempty"`
	StringValueColumn string                `yaml:"string_value_column,omitempty"`
	BatchSize         *int                  `yaml:"batch_size,omitempty"`
	BatchDelayMs      *int                  `yaml:"batch_delay_ms,omitempty"`
	MaxRetries        *int                  `yaml:"max_retries,omitempty"`
	RetryBackoffMs    *int                  `yaml:"retry_backoff_ms,omitempty"`
	KafkaMetadata     *KafkaMetadataMapping `yaml:"kafka_metadata,omitempty"`

	// StartAt optionally configures a per-topic starting position used only
	// when the consumer group has no committed offset for a partition. See
	// StartAt for details.
	StartAt *StartAt `yaml:"start_at,omitempty"`

	// Flatten optionally configures JSON object flattening before insertion,
	// matching Kafka Connect's Flatten$Value SMT behavior.
	Flatten *FlattenConfig `yaml:"flatten,omitempty"`

	// SourcePattern is the topic_patterns entry that produced this mapping, or
	// empty for an explicit topic_tables entry. It is set by TopicPattern.expand,
	// never from YAML.
	SourcePattern string `yaml:"-"`
}

// FlattenConfig configures optional JSON object flattening before insertion.
// When Enabled is true, nested JSON objects are collapsed into a single-level
// map by joining keys with Delimiter, matching Kafka Connect's
// org.apache.kafka.connect.transforms.Flatten$Value SMT behavior.
type FlattenConfig struct {
	Enabled        bool   `yaml:"enabled"`
	Delimiter      string `yaml:"delimiter,omitempty"`
	MaxDepth       int    `yaml:"max_depth,omitempty"`
	PreserveArrays *bool  `yaml:"preserve_arrays,omitempty"`
}

// resolveDefaults fills zero-value fields with Kafka-Connect-compatible defaults.
func (f *FlattenConfig) resolveDefaults() {
	if f == nil {
		return
	}
	if f.Delimiter == "" {
		f.Delimiter = "_"
	}
	if f.PreserveArrays == nil {
		t := true
		f.PreserveArrays = &t
	}
}

// KafkaMetadataMapping configures injection of Kafka message metadata as
// additional columns in the decoded record before it is written to ClickHouse.
// Each field is optional: an empty string means "do not inject that field".
// When a decoded record already contains a key matching one of these column
// names, the metadata value overwrites it and a warning is logged once per
// column per task.
type KafkaMetadataMapping struct {
	Offset    string `yaml:"offset,omitempty"`
	Partition string `yaml:"partition,omitempty"`
	Topic     string `yaml:"topic,omitempty"`
	Timestamp string `yaml:"timestamp,omitempty"`
	Key       string `yaml:"key,omitempty"`
	Headers   string `yaml:"headers,omitempty"`
}

// IsEmpty returns true if no metadata column is configured.
func (k *KafkaMetadataMapping) IsEmpty() bool {
	if k == nil {
		return true
	}
	return k.Offset == "" && k.Partition == "" && k.Topic == "" &&
		k.Timestamp == "" && k.Key == "" && k.Headers == ""
}

// fields returns the configured (name, columnName) pairs in a stable order.
// Empty column names are omitted.
func (k *KafkaMetadataMapping) fields() []struct{ name, column string } {
	if k == nil {
		return nil
	}
	all := []struct{ name, column string }{
		{"offset", k.Offset},
		{"partition", k.Partition},
		{"topic", k.Topic},
		{"timestamp", k.Timestamp},
		{"key", k.Key},
		{"headers", k.Headers},
	}
	out := make([]struct{ name, column string }, 0, len(all))
	for _, f := range all {
		if f.column != "" {
			out = append(out, f)
		}
	}
	return out
}

// Config holds all configuration for the application.
type Config struct {
	KafkaBrokers   string `yaml:"kafka_brokers"`
	SchemaRegistry string `yaml:"schema_registry"`
	ClickHouseDSN  string `yaml:"clickhouse_dsn"`
	GroupID        string `yaml:"group_id"`
	ClientID       string `yaml:"client_id"`
	DLQTopicSuffix string `yaml:"dlq_topic_suffix"`
	InputFormat    string `yaml:"input_format"`

	// String input settings.
	StringValueColumn string `yaml:"string_value_column"`

	// Schema Registry authentication (optional — omit for unauthenticated registries)
	SchemaRegistryUsername string `yaml:"schema_registry_username"`
	SchemaRegistryPassword string `yaml:"schema_registry_password"`

	// Consumer offset reset policy
	AutoOffsetReset string `yaml:"auto_offset_reset"`

	// Kafka authentication (all optional — omit for unauthenticated clusters)
	KafkaSecurityProtocol string `yaml:"kafka_security_protocol"`
	KafkaSASLMechanism    string `yaml:"kafka_sasl_mechanism"`
	KafkaSASLUsername     string `yaml:"kafka_sasl_username"`
	KafkaSASLPassword     string `yaml:"kafka_sasl_password"`
	KafkaSSLCALocation    string `yaml:"kafka_ssl_ca_location"`

	// Server settings
	MetricsPort int `yaml:"metrics_port"`

	// ClickHouse connection pool
	ClickHouseMaxOpenConns int `yaml:"clickhouse_max_open_conns"`
	ClickHouseMaxIdleConns int `yaml:"clickhouse_max_idle_conns"`
	// ClickHouseConnMaxLifetimeS recycles pooled connections after this many seconds.
	// ClickHouse Cloud (and its load balancer) closes idle connections server-side; the
	// clickhouse-go default of 1h is far too long, so the pool hands out dead connections
	// ("connection reset by peer" / "failed to read packet"). Keep this comfortably below
	// the server idle timeout. Must be >= 0; 0 falls back to the default.
	ClickHouseConnMaxLifetimeS int `yaml:"clickhouse_conn_max_lifetime_s"`

	// ClickHouse async insert settings
	ClickHouseAsyncInsert        bool `yaml:"clickhouse_async_insert"`
	ClickHouseWaitForAsyncInsert bool `yaml:"clickhouse_wait_for_async_insert"`

	// Kafka consumer timeout settings
	KafkaSessionTimeoutMs  int `yaml:"kafka_session_timeout_ms"`
	KafkaMaxPollIntervalMs int `yaml:"kafka_max_poll_interval_ms"`

	// Batch defaults (overridable per topic)
	BatchSize      int  `yaml:"batch_size"`
	BatchDelayMs   *int `yaml:"batch_delay_ms"`
	MaxRetries     *int `yaml:"max_retries"`
	RetryBackoffMs *int `yaml:"retry_backoff_ms"`

	// Shutdown timeout (seconds) for flushing remaining batches and stopping the HTTP server
	ShutdownTimeoutS int `yaml:"shutdown_timeout_s"`

	// Auto-restart supervision for transiently-crashed sink tasks.
	AutoRestart AutoRestartConfig `yaml:"auto_restart"`

	// ReadinessGraceS is how long after launch a running sink task may have no
	// partition assignment before /readyz reports it. Joining a consumer group
	// takes a few seconds, and discovered topics spawn tasks at any time, so
	// without a grace window readiness would flap on every new topic. Nil
	// defaults to 30; *0 disables the grace (previous behavior).
	ReadinessGraceS *int `yaml:"readiness_grace_s"`

	// Topic discovery: how often topic_patterns are reconciled against the
	// Kafka cluster and parked table_missing topics are retried.
	TopicDiscovery TopicDiscoveryConfig `yaml:"topic_discovery"`

	TopicTables   []TopicTableMapping `yaml:"topic_tables"`
	TopicPatterns []TopicPattern      `yaml:"topic_patterns"`
}

// TopicDiscoveryConfig controls the reconcile loop that expands topic_patterns
// into sink tasks and retries topics whose target table did not exist yet.
type TopicDiscoveryConfig struct {
	// IntervalS is the seconds between discovery ticks. Nil defaults to 60.
	// *0 disables the loop: patterns are still expanded once at startup, but
	// topics created later are only picked up on a restart or via
	// POST /api/discovery/run.
	IntervalS *int `yaml:"interval_s"`
	// MaxTopics caps the number of pattern-discovered sink tasks (running or
	// stopped; parked table_missing topics hold no consumer and do not count).
	// Every task is a full librdkafka consumer with its own prefetch buffer, so
	// an over-broad pattern on a large cluster would otherwise exhaust the pod's
	// memory. 0 means "use the default" (200).
	MaxTopics int `yaml:"max_topics"`
	// TimeoutMs bounds each Kafka metadata request and ClickHouse DESCRIBE issued
	// by a discovery tick. 0 means "use the default" (5000).
	TimeoutMs int `yaml:"timeout_ms"`
}

// AutoRestartConfig controls the TaskManager supervisor that restarts sink tasks
// which stop with a transient (recoverable) error, instead of leaving them stopped
// until an operator intervenes. Crashes classified as fatal (poison messages in
// strict mode, non-retriable ClickHouse errors) are never auto-restarted.
type AutoRestartConfig struct {
	// Enabled turns the supervisor on. Nil defaults to true.
	Enabled *bool `yaml:"enabled"`
	// InitialBackoffMs is the delay before the first restart; it doubles each
	// consecutive failure up to MaxBackoffMs (with jitter).
	InitialBackoffMs int `yaml:"initial_backoff_ms"`
	// MaxBackoffMs caps the restart backoff.
	MaxBackoffMs int `yaml:"max_backoff_ms"`
	// ResetAfterS is the healthy uptime after which a restarted task's consecutive
	// failure counter is reset to zero.
	ResetAfterS int `yaml:"reset_after_s"`
	// MaxStuckS is the longest a task may keep crash-looping before the supervisor
	// gives up and escalates to a pod recycle via /livez. A negative value disables
	// escalation (the supervisor keeps retrying with capped backoff forever). 0 means
	// "use the default".
	MaxStuckS int `yaml:"max_stuck_s"`
}

// intPtr returns a pointer to a copy of v.
func intPtr(v int) *int { return &v }

// boolPtr returns a pointer to a copy of v.
func boolPtr(v bool) *bool { return &v }

// resolve fills nil pointer fields from global defaults.
// A nil pointer means "not configured by the user"; a non-nil pointer (including *0) is respected.
func (m *TopicTableMapping) resolve(cfg *Config) {
	m.Format = normalizeInputFormat(m.Format)
	if m.Format == "" {
		m.Format = cfg.InputFormat
	}
	m.StringValueColumn = strings.TrimSpace(m.StringValueColumn)
	if m.StringValueColumn == "" {
		m.StringValueColumn = cfg.StringValueColumn
	}
	if m.BatchSize == nil {
		m.BatchSize = intPtr(cfg.BatchSize)
	}
	if m.BatchDelayMs == nil {
		m.BatchDelayMs = cfg.BatchDelayMs
	}
	if m.MaxRetries == nil {
		m.MaxRetries = cfg.MaxRetries
	}
	if m.RetryBackoffMs == nil {
		m.RetryBackoffMs = cfg.RetryBackoffMs
	}
	if m.KafkaMetadata != nil {
		m.KafkaMetadata.Offset = strings.TrimSpace(m.KafkaMetadata.Offset)
		m.KafkaMetadata.Partition = strings.TrimSpace(m.KafkaMetadata.Partition)
		m.KafkaMetadata.Topic = strings.TrimSpace(m.KafkaMetadata.Topic)
		m.KafkaMetadata.Timestamp = strings.TrimSpace(m.KafkaMetadata.Timestamp)
		m.KafkaMetadata.Key = strings.TrimSpace(m.KafkaMetadata.Key)
		m.KafkaMetadata.Headers = strings.TrimSpace(m.KafkaMetadata.Headers)
	}
	if m.Flatten != nil {
		m.Flatten.resolveDefaults()
	}
}

func applyDefaults(cfg *Config) {
	if cfg.KafkaBrokers == "" {
		cfg.KafkaBrokers = "localhost:9092"
	}
	if cfg.SchemaRegistry == "" {
		cfg.SchemaRegistry = "http://localhost:8081"
	}
	if cfg.ClickHouseDSN == "" {
		cfg.ClickHouseDSN = "tcp://localhost:9000"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "kahouse"
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "kahouse"
	}
	if cfg.DLQTopicSuffix == "" {
		cfg.DLQTopicSuffix = ".dlq"
	}
	if cfg.InputFormat == "" {
		cfg.InputFormat = "avro"
	}
	if cfg.StringValueColumn == "" {
		cfg.StringValueColumn = "value"
	}
	if cfg.AutoOffsetReset == "" {
		cfg.AutoOffsetReset = "earliest"
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 10000
	}
	if cfg.BatchDelayMs == nil {
		cfg.BatchDelayMs = intPtr(200)
	}
	if cfg.MaxRetries == nil {
		cfg.MaxRetries = intPtr(5)
	}
	if cfg.RetryBackoffMs == nil {
		cfg.RetryBackoffMs = intPtr(100)
	}
	if cfg.MetricsPort == 0 {
		cfg.MetricsPort = 9090
	}
	if cfg.ClickHouseMaxOpenConns == 0 {
		cfg.ClickHouseMaxOpenConns = 5
	}
	if cfg.ClickHouseMaxIdleConns == 0 {
		cfg.ClickHouseMaxIdleConns = 5
	}
	if cfg.ClickHouseConnMaxLifetimeS == 0 {
		cfg.ClickHouseConnMaxLifetimeS = 300 // 5 min; ClickHouse Cloud closes idle conns well before the 1h driver default
	}
	// ClickHouseAsyncInsert and ClickHouseWaitForAsyncInsert default to false (zero value for bool).
	// This is intentional: async inserts are opt-in.
	if cfg.KafkaSessionTimeoutMs == 0 {
		cfg.KafkaSessionTimeoutMs = 45000
	}
	if cfg.KafkaMaxPollIntervalMs == 0 {
		cfg.KafkaMaxPollIntervalMs = 300000
	}
	if cfg.ShutdownTimeoutS == 0 {
		cfg.ShutdownTimeoutS = 5
	}
	if cfg.AutoRestart.Enabled == nil {
		cfg.AutoRestart.Enabled = boolPtr(true)
	}
	if cfg.AutoRestart.InitialBackoffMs == 0 {
		cfg.AutoRestart.InitialBackoffMs = 5000
	}
	if cfg.AutoRestart.MaxBackoffMs == 0 {
		cfg.AutoRestart.MaxBackoffMs = 300000 // 5 min
	}
	if cfg.AutoRestart.ResetAfterS == 0 {
		cfg.AutoRestart.ResetAfterS = 120
	}
	if cfg.AutoRestart.MaxStuckS == 0 {
		cfg.AutoRestart.MaxStuckS = 900 // 15 min, then escalate to pod recycle
	}
	if cfg.ReadinessGraceS == nil {
		cfg.ReadinessGraceS = intPtr(30)
	}
	if cfg.TopicDiscovery.IntervalS == nil {
		cfg.TopicDiscovery.IntervalS = intPtr(60)
	}
	if cfg.TopicDiscovery.MaxTopics == 0 {
		cfg.TopicDiscovery.MaxTopics = 200
	}
	if cfg.TopicDiscovery.TimeoutMs == 0 {
		cfg.TopicDiscovery.TimeoutMs = 5000
	}
}

// ConfigPath returns the config file path from -config flag or KAHOUSE_CONFIG env var.
// Defaults to "kahouse.yaml". Call flag.Parse() before calling this.
func ConfigPath() string {
	if *configFlag != "" {
		return *configFlag
	}
	if env := os.Getenv("KAHOUSE_CONFIG"); env != "" {
		return env
	}
	return "kahouse.yaml"
}

var configFlag = flag.String("config", "", "path to config file (default: kahouse.yaml or KAHOUSE_CONFIG)")

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %q: %w", path, err)
	}

	applyDefaults(&cfg)
	cfg.InputFormat = normalizeInputFormat(cfg.InputFormat)
	cfg.StringValueColumn = strings.TrimSpace(cfg.StringValueColumn)

	for i := range cfg.TopicTables {
		cfg.TopicTables[i].resolve(&cfg)
	}
	for i := range cfg.TopicPatterns {
		cfg.TopicPatterns[i].resolve(&cfg)
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func validateConfig(cfg *Config) error {
	switch cfg.InputFormat {
	case "avro", "json", "string":
	case "":
		return fmt.Errorf("input_format is required")
	default:
		return fmt.Errorf("input_format must be one of avro, json, or string, got %q", cfg.InputFormat)
	}

	switch cfg.AutoOffsetReset {
	case "earliest", "latest", "none":
	default:
		return fmt.Errorf("auto_offset_reset must be one of earliest, latest, or none, got %q", cfg.AutoOffsetReset)
	}

	if strings.TrimSpace(cfg.KafkaBrokers) == "" {
		return fmt.Errorf("kafka_brokers is required")
	}
	if anyTopicUsesFormat(cfg, "avro") && strings.TrimSpace(cfg.SchemaRegistry) == "" {
		return fmt.Errorf("schema_registry is required")
	}
	if strings.TrimSpace(cfg.ClickHouseDSN) == "" {
		return fmt.Errorf("clickhouse_dsn is required")
	}
	if strings.TrimSpace(cfg.GroupID) == "" {
		return fmt.Errorf("group_id is required")
	}
	if strings.TrimSpace(cfg.DLQTopicSuffix) == "" {
		return fmt.Errorf("dlq_topic_suffix is required and must not be empty")
	}
	if cfg.InputFormat == "string" && cfg.StringValueColumn == "" {
		return fmt.Errorf("string_value_column is required when input_format is string")
	}
	if cfg.ClickHouseMaxOpenConns < 1 {
		return fmt.Errorf("clickhouse_max_open_conns must be at least 1, got %d", cfg.ClickHouseMaxOpenConns)
	}
	if cfg.ClickHouseMaxIdleConns < 1 {
		return fmt.Errorf("clickhouse_max_idle_conns must be at least 1, got %d", cfg.ClickHouseMaxIdleConns)
	}
	if cfg.ClickHouseConnMaxLifetimeS < 0 {
		return fmt.Errorf("clickhouse_conn_max_lifetime_s must be >= 0, got %d", cfg.ClickHouseConnMaxLifetimeS)
	}
	if cfg.BatchSize < 1 {
		return fmt.Errorf("batch_size must be at least 1, got %d", cfg.BatchSize)
	}
	if *cfg.BatchDelayMs < 0 {
		return fmt.Errorf("batch_delay_ms must be >= 0, got %d", *cfg.BatchDelayMs)
	}
	if *cfg.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be >= 0, got %d", *cfg.MaxRetries)
	}
	if *cfg.RetryBackoffMs < 0 {
		return fmt.Errorf("retry_backoff_ms must be >= 0, got %d", *cfg.RetryBackoffMs)
	}
	if cfg.KafkaSessionTimeoutMs <= 0 {
		return fmt.Errorf("kafka_session_timeout_ms must be > 0, got %d", cfg.KafkaSessionTimeoutMs)
	}
	if cfg.KafkaMaxPollIntervalMs <= 0 {
		return fmt.Errorf("kafka_max_poll_interval_ms must be > 0, got %d", cfg.KafkaMaxPollIntervalMs)
	}
	if cfg.ShutdownTimeoutS <= 0 {
		return fmt.Errorf("shutdown_timeout_s must be > 0, got %d", cfg.ShutdownTimeoutS)
	}
	if cfg.AutoRestart.InitialBackoffMs < 0 {
		return fmt.Errorf("auto_restart.initial_backoff_ms must be >= 0, got %d", cfg.AutoRestart.InitialBackoffMs)
	}
	if cfg.AutoRestart.MaxBackoffMs < cfg.AutoRestart.InitialBackoffMs {
		return fmt.Errorf("auto_restart.max_backoff_ms (%d) must be >= initial_backoff_ms (%d)", cfg.AutoRestart.MaxBackoffMs, cfg.AutoRestart.InitialBackoffMs)
	}
	if cfg.AutoRestart.ResetAfterS < 0 {
		return fmt.Errorf("auto_restart.reset_after_s must be >= 0, got %d", cfg.AutoRestart.ResetAfterS)
	}
	if cfg.ReadinessGraceS != nil && *cfg.ReadinessGraceS < 0 {
		return fmt.Errorf("readiness_grace_s must be >= 0, got %d", *cfg.ReadinessGraceS)
	}
	if cfg.TopicDiscovery.IntervalS != nil && *cfg.TopicDiscovery.IntervalS < 0 {
		return fmt.Errorf("topic_discovery.interval_s must be >= 0, got %d", *cfg.TopicDiscovery.IntervalS)
	}
	if cfg.TopicDiscovery.MaxTopics < 1 {
		return fmt.Errorf("topic_discovery.max_topics must be at least 1, got %d", cfg.TopicDiscovery.MaxTopics)
	}
	if cfg.TopicDiscovery.TimeoutMs <= 0 {
		return fmt.Errorf("topic_discovery.timeout_ms must be > 0, got %d", cfg.TopicDiscovery.TimeoutMs)
	}
	if len(cfg.TopicTables) == 0 && len(cfg.TopicPatterns) == 0 {
		return fmt.Errorf("at least one topic_tables mapping or topic_patterns entry is required")
	}
	seenTopics := make(map[string]int, len(cfg.TopicTables))
	for i := range cfg.TopicTables {
		tt := &cfg.TopicTables[i]
		where := fmt.Sprintf("topic_tables[%d]", i)
		trimmedTopic := strings.TrimSpace(tt.Topic)
		if trimmedTopic == "" {
			return fmt.Errorf("%s: topic is required", where)
		}
		if prev, exists := seenTopics[trimmedTopic]; exists {
			return fmt.Errorf("%s: duplicate topic %q (first defined at index %d)", where, trimmedTopic, prev)
		}
		seenTopics[trimmedTopic] = i
		if strings.TrimSpace(tt.Table) == "" {
			return fmt.Errorf("%s: table is required", where)
		}
		if err := validateMapping(tt); err != nil {
			return fmt.Errorf("%s: %w", where, err)
		}
	}
	seenPatterns := make(map[string]int, len(cfg.TopicPatterns))
	for i := range cfg.TopicPatterns {
		p := &cfg.TopicPatterns[i]
		where := fmt.Sprintf("topic_patterns[%d]", i)
		if strings.TrimSpace(p.Topic) != "" {
			return fmt.Errorf("%s: topic is not allowed here, use pattern (or move the entry to topic_tables)", where)
		}
		if err := p.compile(); err != nil {
			return fmt.Errorf("%s: %w", where, err)
		}
		trimmedPattern := strings.TrimSpace(p.Pattern)
		if prev, exists := seenPatterns[trimmedPattern]; exists {
			return fmt.Errorf("%s: duplicate pattern %q (first defined at index %d)", where, trimmedPattern, prev)
		}
		seenPatterns[trimmedPattern] = i
		if err := validateMapping(&p.TopicTableMapping); err != nil {
			return fmt.Errorf("%s: %w", where, err)
		}
	}

	return nil
}

// validateMapping checks the per-topic fields shared by a topic_tables entry and
// the template embedded in a topic_patterns entry. The mapping must already be
// resolved so the pointer fields are non-nil.
func validateMapping(tt *TopicTableMapping) error {
	switch tt.Format {
	case "avro", "json", "string":
	default:
		return fmt.Errorf("format must be one of avro, json, or string, got %q", tt.Format)
	}
	if tt.Format == "string" && tt.StringValueColumn == "" {
		return fmt.Errorf("string_value_column is required when format is string")
	}
	if *tt.BatchSize < 1 {
		return fmt.Errorf("batch_size must be at least 1, got %d", *tt.BatchSize)
	}
	if *tt.BatchDelayMs < 0 {
		return fmt.Errorf("batch_delay_ms must be >= 0, got %d", *tt.BatchDelayMs)
	}
	if *tt.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be >= 0, got %d", *tt.MaxRetries)
	}
	if *tt.RetryBackoffMs < 0 {
		return fmt.Errorf("retry_backoff_ms must be >= 0, got %d", *tt.RetryBackoffMs)
	}
	if tt.StartAt != nil {
		if err := tt.StartAt.validate(); err != nil {
			return err
		}
	}
	if !tt.KafkaMetadata.IsEmpty() {
		seenCols := make(map[string]string, 6)
		for _, f := range tt.KafkaMetadata.fields() {
			if prev, exists := seenCols[f.column]; exists {
				return fmt.Errorf("kafka_metadata has duplicate column name %q (used by both %q and %q)", f.column, prev, f.name)
			}
			seenCols[f.column] = f.name
		}
	}
	if tt.Flatten != nil && tt.Flatten.Enabled {
		if tt.Format != "json" {
			return fmt.Errorf("flatten is only supported with json format, got %q", tt.Format)
		}
		if tt.Flatten.Delimiter == "" {
			return fmt.Errorf("flatten.delimiter must not be empty when flatten is enabled")
		}
		if tt.Flatten.MaxDepth < 0 {
			return fmt.Errorf("flatten.max_depth must be >= 0, got %d", tt.Flatten.MaxDepth)
		}
	}
	return nil
}

func configLogFields(cfg *Config) []interface{} {
	return []interface{}{
		"kafka_brokers", cfg.KafkaBrokers,
		"schema_registry", cfg.SchemaRegistry,
		"schema_registry_username", cfg.SchemaRegistryUsername,
		"schema_registry_password", redactSecret(cfg.SchemaRegistryPassword),
		"clickhouse_dsn", redactDSN(cfg.ClickHouseDSN),
		"clickhouse_max_open_conns", cfg.ClickHouseMaxOpenConns,
		"clickhouse_max_idle_conns", cfg.ClickHouseMaxIdleConns,
		"clickhouse_conn_max_lifetime_s", cfg.ClickHouseConnMaxLifetimeS,
		"clickhouse_async_insert", cfg.ClickHouseAsyncInsert,
		"clickhouse_wait_for_async_insert", cfg.ClickHouseWaitForAsyncInsert,
		"group_id", cfg.GroupID,
		"client_id", cfg.ClientID,
		"dlq_topic_suffix", cfg.DLQTopicSuffix,
		"input_format", cfg.InputFormat,
		"string_value_column", cfg.StringValueColumn,
		"auto_offset_reset", cfg.AutoOffsetReset,
		"kafka_security_protocol", cfg.KafkaSecurityProtocol,
		"kafka_sasl_mechanism", cfg.KafkaSASLMechanism,
		"kafka_sasl_username", cfg.KafkaSASLUsername,
		"kafka_sasl_password", redactSecret(cfg.KafkaSASLPassword),
		"kafka_ssl_ca_location", cfg.KafkaSSLCALocation,
		"batch_size", cfg.BatchSize,
		"batch_delay_ms", *cfg.BatchDelayMs,
		"max_retries", *cfg.MaxRetries,
		"retry_backoff_ms", *cfg.RetryBackoffMs,
		"kafka_session_timeout_ms", cfg.KafkaSessionTimeoutMs,
		"kafka_max_poll_interval_ms", cfg.KafkaMaxPollIntervalMs,
		"shutdown_timeout_s", cfg.ShutdownTimeoutS,
		"auto_restart_enabled", cfg.AutoRestart.Enabled != nil && *cfg.AutoRestart.Enabled,
		"auto_restart_initial_backoff_ms", cfg.AutoRestart.InitialBackoffMs,
		"auto_restart_max_backoff_ms", cfg.AutoRestart.MaxBackoffMs,
		"auto_restart_reset_after_s", cfg.AutoRestart.ResetAfterS,
		"auto_restart_max_stuck_s", cfg.AutoRestart.MaxStuckS,
		"readiness_grace_s", derefInt(cfg.ReadinessGraceS),
		"topic_discovery_interval_s", derefInt(cfg.TopicDiscovery.IntervalS),
		"topic_discovery_max_topics", cfg.TopicDiscovery.MaxTopics,
		"topic_discovery_timeout_ms", cfg.TopicDiscovery.TimeoutMs,
		"topic_tables", cfg.TopicTables,
		"topic_patterns", cfg.TopicPatterns,
	}
}

// derefInt returns *p, or 0 for a nil pointer (a field whose default was not applied).
func derefInt(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

func normalizeInputFormat(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// anyTopicUsesFormat reports whether any explicit mapping or pattern template
// uses the given input format. Patterns count because every topic they discover
// inherits the template's format, so e.g. an avro-only pattern config still
// needs a Schema Registry client.
func anyTopicUsesFormat(cfg *Config, format string) bool {
	for _, tt := range cfg.TopicTables {
		if tt.Format == format {
			return true
		}
	}
	for _, p := range cfg.TopicPatterns {
		if p.Format == format {
			return true
		}
	}
	return false
}

func redactSecret(value string) string {
	if value == "" {
		return ""
	}
	return "[redacted]"
}

// sanitizeDSN percent-encodes the user and password portions of a DSN so that
// special characters (?, @, #, etc.) in credentials don't break url.Parse.
// It locates the userinfo section between "://" and the last "@", splits on
// the first ":" to separate user from password, and encodes both using
// url.UserPassword which correctly escapes characters reserved in userinfo.
func sanitizeDSN(dsn string) string {
	schemeEnd := strings.Index(dsn, "://")
	if schemeEnd == -1 {
		return dsn
	}

	rest := dsn[schemeEnd+3:]
	lastAt := strings.LastIndex(rest, "@")
	if lastAt == -1 {
		return dsn // no userinfo
	}

	userinfo := rest[:lastAt]
	hostAndRest := rest[lastAt+1:]
	scheme := dsn[:schemeEnd+3]

	colonIdx := strings.Index(userinfo, ":")
	if colonIdx == -1 {
		// Username only, no password.
		encoded := url.User(userinfo).String()
		return scheme + encoded + "@" + hostAndRest
	}

	user := userinfo[:colonIdx]
	pass := userinfo[colonIdx+1:]

	encoded := url.UserPassword(user, pass).String()
	return scheme + encoded + "@" + hostAndRest
}

func redactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	if idx := strings.Index(dsn, "@"); idx != -1 {
		if schemeIdx := strings.Index(dsn, "://"); schemeIdx != -1 && schemeIdx < idx {
			return dsn[:schemeIdx+3] + "[redacted]" + dsn[idx:]
		}
	}
	return dsn
}

// buildKafkaConfig applies auth fields from cfg onto a base kafka.ConfigMap.
// Fields are only set when non-empty, so omitting them keeps existing unauthenticated behavior.
func buildKafkaConfig(base kafka.ConfigMap, cfg *Config) kafka.ConfigMap {
	if cfg.KafkaSecurityProtocol != "" {
		base["security.protocol"] = cfg.KafkaSecurityProtocol
	}
	if cfg.KafkaSASLMechanism != "" {
		base["sasl.mechanism"] = cfg.KafkaSASLMechanism
	}
	if cfg.KafkaSASLUsername != "" {
		base["sasl.username"] = cfg.KafkaSASLUsername
	}
	if cfg.KafkaSASLPassword != "" {
		base["sasl.password"] = cfg.KafkaSASLPassword
	}
	if cfg.KafkaSSLCALocation != "" {
		base["ssl.ca.location"] = cfg.KafkaSSLCALocation
	}
	return base
}
