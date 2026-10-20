package app

import (
	"strings"
	"testing"
)

func TestTopicPatternCompileRejects(t *testing.T) {
	tests := []struct {
		name    string
		pattern TopicPattern
		want    string
	}{
		{
			name:    "empty pattern",
			pattern: TopicPattern{Pattern: "  ", TopicTableMapping: TopicTableMapping{Table: "db.t"}},
			want:    "pattern is required",
		},
		{
			name:    "invalid regex",
			pattern: TopicPattern{Pattern: `prod\.(`, TopicTableMapping: TopicTableMapping{Table: "db.t"}},
			want:    "error parsing regexp",
		},
		{
			name:    "empty table",
			pattern: TopicPattern{Pattern: `prod\..*`},
			want:    "table is required",
		},
		{
			name:    "unknown named group",
			pattern: TopicPattern{Pattern: `prod\.(?P<d>[a-z]+)`, TopicTableMapping: TopicTableMapping{Table: "db.${nope}"}},
			want:    `references group "nope"`,
		},
		{
			name:    "numeric group out of range",
			pattern: TopicPattern{Pattern: `prod\.([a-z]+)`, TopicTableMapping: TopicTableMapping{Table: "db.${2}"}},
			want:    `references group "2"`,
		},
		{
			name:    "bare dollar reference",
			pattern: TopicPattern{Pattern: `prod\.(?P<d>[a-z]+)`, TopicTableMapping: TopicTableMapping{Table: "db.$d"}},
			want:    "bare $",
		},
		{
			name:    "trailing dollar",
			pattern: TopicPattern{Pattern: `prod\..*`, TopicTableMapping: TopicTableMapping{Table: "db.t$"}},
			want:    "bare $",
		},
		{
			name:    "unterminated brace",
			pattern: TopicPattern{Pattern: `prod\.(?P<d>[a-z]+)`, TopicTableMapping: TopicTableMapping{Table: "db.${d"}},
			want:    "unterminated",
		},
		{
			name:    "empty reference",
			pattern: TopicPattern{Pattern: `prod\..*`, TopicTableMapping: TopicTableMapping{Table: "db.${}"}},
			want:    `references group ""`,
		},
		{
			name:    "empty replace key",
			pattern: TopicPattern{Pattern: `prod\..*`, TableReplace: map[string]string{"": "_"}, TopicTableMapping: TopicTableMapping{Table: "db.${0}"}},
			want:    "table_replace: keys must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pattern.compile()
			if err == nil {
				t.Fatalf("Expected error containing %q, got nil", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %q", tt.want, err.Error())
			}
		})
	}
}

func TestTopicPatternCompileIsIdempotent(t *testing.T) {
	p := TopicPattern{Pattern: `prod\..*`, TopicTableMapping: TopicTableMapping{Table: "db.${0}"}}
	if err := p.compile(); err != nil {
		t.Fatalf("first compile: %v", err)
	}
	re := p.re
	if err := p.compile(); err != nil {
		t.Fatalf("second compile: %v", err)
	}
	if p.re != re {
		t.Fatal("Expected second compile to keep the compiled regexp")
	}
}

func TestTopicPatternExpand(t *testing.T) {
	batch := 500
	p := TopicPattern{
		Pattern: `prod\.(?P<domain>[a-z]+)\.(?P<entity>[a-z_]+)`,
		TopicTableMapping: TopicTableMapping{
			Table:     "analytics.${domain}_${entity}",
			Format:    "json",
			BatchSize: &batch,
		},
	}
	if err := p.compile(); err != nil {
		t.Fatalf("compile: %v", err)
	}

	m, ok := p.expand("prod.hr.employees")
	if !ok {
		t.Fatal("Expected prod.hr.employees to match")
	}
	if m.Topic != "prod.hr.employees" {
		t.Errorf("Topic = %q, want prod.hr.employees", m.Topic)
	}
	if m.Table != "analytics.hr_employees" {
		t.Errorf("Table = %q, want analytics.hr_employees", m.Table)
	}
	if m.Format != "json" || m.BatchSize == nil || *m.BatchSize != 500 {
		t.Errorf("Expected overrides to be copied onto the mapping, got format=%q batch_size=%v", m.Format, m.BatchSize)
	}
	if m.SourcePattern != p.Pattern {
		t.Errorf("SourcePattern = %q, want %q", m.SourcePattern, p.Pattern)
	}
	if p.Table != "analytics.${domain}_${entity}" || p.Topic != "" {
		t.Errorf("expand must not mutate the pattern's template, got table=%q topic=%q", p.Table, p.Topic)
	}

	// Full-match anchoring: prefixes, suffixes and other environments never match.
	for _, topic := range []string{"prod.hr.employees.dlq", "staging.hr.employees", "prod.hr.employees_v2", "xprod.hr.employees", ""} {
		if _, ok := p.expand(topic); ok {
			t.Errorf("Expected %q not to match", topic)
		}
	}
}

func TestTopicPatternExpandCarriesTombstones(t *testing.T) {
	p := TopicPattern{
		Pattern: `prod\.(?P<domain>[a-z]+)\.snapshots`,
		TopicTableMapping: TopicTableMapping{
			Table:         "analytics.${domain}_snapshots",
			KafkaMetadata: &KafkaMetadataMapping{Offset: "__offset"},
			Tombstones:    &TombstoneConfig{Mode: " row ", KeyColumn: "id"},
		},
	}
	cfg := validConfig()
	p.resolve(&cfg)
	if err := p.compile(); err != nil {
		t.Fatalf("compile: %v", err)
	}

	m, ok := p.expand("prod.teams.snapshots")
	if !ok {
		t.Fatal("Expected prod.teams.snapshots to match")
	}
	if !m.writesTombstoneRows() {
		t.Fatalf("Expected the discovered topic to write deletion rows, got %+v", m.Tombstones)
	}
	if m.Tombstones.KeyColumn != "id" || m.Tombstones.DeletedColumn != DefaultTombstoneDeletedColumn {
		t.Errorf("Expected resolved tombstones config, got %+v", m.Tombstones)
	}
}

func TestTopicPatternExpandTemplateForms(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		table   string
		replace map[string]string
		topic   string
		want    string
	}{
		{"whole match", `prod\..*`, "analytics.${0}", nil, "prod.hr.employees", "analytics.prod.hr.employees"},
		{"numeric group", `cdc\.public\.(.+)`, "raw.${1}", nil, "cdc.public.invoices", "raw.invoices"},
		{"adjacent references", `(\w+)\.(\w+)`, "${1}${2}", nil, "a.b", "ab"},
		{"literal dollar", `(.+)`, "db.$$${1}", nil, "orders", "db.$orders"},
		{"replace inside a group", `prod\.(.+)`, "analytics.${1}", map[string]string{".": "_"}, "prod.hr.employees", "analytics.hr_employees"},
		{"replace inside the whole match", `prod\..*`, "analytics.${0}", map[string]string{".": "_"}, "prod.hr.employees", "analytics.prod_hr_employees"},
		{"replace never touches template literals", `prod\.(.+)`, "analytics.${1}", map[string]string{"analytics": "x", ".": "_"}, "prod.hr.employees", "analytics.hr_employees"},
		{"user anchors are harmless", `^prod\.(.+)$`, "db.${1}", nil, "prod.hr", "db.hr"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := TopicPattern{Pattern: tt.pattern, TableReplace: tt.replace, TopicTableMapping: TopicTableMapping{Table: tt.table}}
			if err := p.compile(); err != nil {
				t.Fatalf("compile: %v", err)
			}
			m, ok := p.expand(tt.topic)
			if !ok {
				t.Fatalf("Expected %q to match %q", tt.topic, tt.pattern)
			}
			if m.Table != tt.want {
				t.Fatalf("Table = %q, want %q", m.Table, tt.want)
			}
		})
	}
}
