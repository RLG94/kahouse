package app

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// TopicPattern maps every Kafka topic matching a regular expression to a
// ClickHouse table derived from the topic name. Matching topics are discovered
// from cluster metadata at startup and on every discovery tick, and each one
// gets its own SinkTask, Kafka consumer and consumer group, exactly as if it
// had been listed in topic_tables. A failure in one discovered topic therefore
// stops only that topic's task.
//
// The embedded TopicTableMapping carries the per-topic overrides (format,
// batch sizes, start_at, kafka_metadata, flatten, ...) that apply to every
// topic the pattern produces. Its Table field holds the table template and its
// Topic field must stay empty.
type TopicPattern struct {
	// Pattern is an RE2 regular expression matched against the whole topic
	// name. It is anchored automatically, so "events" does not match
	// "events.dlq".
	Pattern string `yaml:"pattern"`
	// TableReplace rewrites substrings inside every captured group, including
	// ${0}, before the table template is expanded. {".": "_"} turns
	// "prod.hr.employees" into "prod_hr_employees".
	TableReplace map[string]string `yaml:"table_replace,omitempty"`

	TopicTableMapping `yaml:",inline"`

	re       *regexp.Regexp
	template []templatePart
	replacer *strings.Replacer
}

// templatePart is one piece of a parsed table template: literal text, or a
// reference to a capture group resolved to its index.
type templatePart struct {
	literal string
	group   int
	isRef   bool
}

// compile anchors and compiles the pattern, parses the table template against
// the pattern's capture groups and builds the table_replace replacer. It is
// idempotent and must succeed before expand is called.
func (p *TopicPattern) compile() error {
	if p.re != nil {
		return nil
	}
	trimmed := strings.TrimSpace(p.Pattern)
	if trimmed == "" {
		return fmt.Errorf("pattern is required")
	}
	re, err := regexp.Compile("^(?:" + trimmed + ")$")
	if err != nil {
		return fmt.Errorf("pattern %q: %w", p.Pattern, err)
	}
	if strings.TrimSpace(p.Table) == "" {
		return fmt.Errorf("table is required")
	}
	template, err := parseTableTemplate(p.Table, re)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(p.TableReplace))
	for k := range p.TableReplace {
		if k == "" {
			return fmt.Errorf("table_replace: keys must not be empty")
		}
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic replacer construction
	pairs := make([]string, 0, 2*len(keys))
	for _, k := range keys {
		pairs = append(pairs, k, p.TableReplace[k])
	}

	p.re = re
	p.template = template
	if len(pairs) > 0 {
		p.replacer = strings.NewReplacer(pairs...)
	}
	return nil
}

// parseTableTemplate splits tpl into literal text and capture-group references
// and checks every reference against the groups re defines. Only the braced
// forms ${N} and ${name} are accepted, plus $$ for a literal dollar sign. A bare
// $name is rejected on purpose: regexp.Expand reads "$domain_$entity" as a
// reference to a group called "domain_", which silently expands to nothing.
func parseTableTemplate(tpl string, re *regexp.Regexp) ([]templatePart, error) {
	groups := make(map[string]int, 2*(re.NumSubexp()+1))
	for i, name := range re.SubexpNames() {
		groups[strconv.Itoa(i)] = i
		if name != "" {
			groups[name] = i
		}
	}

	var parts []templatePart
	var literal strings.Builder
	flush := func() {
		if literal.Len() > 0 {
			parts = append(parts, templatePart{literal: literal.String()})
			literal.Reset()
		}
	}

	for i := 0; i < len(tpl); i++ {
		c := tpl[i]
		if c != '$' {
			literal.WriteByte(c)
			continue
		}
		if i+1 < len(tpl) && tpl[i+1] == '$' {
			literal.WriteByte('$')
			i++
			continue
		}
		if i+1 >= len(tpl) || tpl[i+1] != '{' {
			return nil, fmt.Errorf("table template %q: bare $ at offset %d; write group references as ${name} or ${N} and a literal dollar as $$", tpl, i)
		}
		end := strings.IndexByte(tpl[i+2:], '}')
		if end == -1 {
			return nil, fmt.Errorf("table template %q: unterminated ${ at offset %d", tpl, i)
		}
		ref := tpl[i+2 : i+2+end]
		idx, ok := groups[ref]
		if !ok {
			return nil, fmt.Errorf("table template %q references group %q, which the pattern does not define", tpl, ref)
		}
		flush()
		parts = append(parts, templatePart{group: idx, isRef: true})
		i += 2 + end
	}
	flush()
	return parts, nil
}

// expand returns the concrete mapping for topic, or false when it does not
// match. The result is a copy of the pattern's overrides with Topic set, Table
// expanded from the template and SourcePattern recording where it came from.
func (p *TopicPattern) expand(topic string) (TopicTableMapping, bool) {
	sub := p.re.FindStringSubmatch(topic)
	if sub == nil {
		return TopicTableMapping{}, false
	}
	var table strings.Builder
	for _, part := range p.template {
		if !part.isRef {
			table.WriteString(part.literal)
			continue
		}
		group := sub[part.group]
		if p.replacer != nil {
			group = p.replacer.Replace(group)
		}
		table.WriteString(group)
	}
	m := p.TopicTableMapping
	m.Topic = topic
	m.Table = table.String()
	m.SourcePattern = p.Pattern
	return m, true
}
