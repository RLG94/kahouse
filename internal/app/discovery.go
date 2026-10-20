package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// discoveryDeleteAfterMisses is how many consecutive successful listings a
// pattern-discovered topic must be absent from before its task is removed.
// Two ticks absorb a metadata flap without tearing down a healthy consumer.
const discoveryDeleteAfterMisses = 2

// topicLister returns the names of every topic in the Kafka cluster.
type topicLister interface {
	ListTopics(ctx context.Context) ([]string, error)
}

// kafkaTopicLister lists topics with the admin client's metadata request, the
// same call validateDLQTopics uses. librdkafka bounds the call by timeout, so a
// tick against unreachable brokers fails fast instead of hanging.
type kafkaTopicLister struct {
	admin   *kafka.AdminClient
	timeout time.Duration
}

func (l *kafkaTopicLister) ListTopics(context.Context) ([]string, error) {
	md, err := l.admin.GetMetadata(nil, true, int(l.timeout/time.Millisecond))
	if err != nil {
		return nil, err
	}
	// A topic-level error (e.g. a leader election on a brand-new topic) still
	// means the topic exists, so every name is returned.
	names := make([]string, 0, len(md.Topics))
	for name := range md.Topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// DiscoveryResult summarises one reconcile tick. It is logged at startup and
// returned by POST /api/discovery/run.
type DiscoveryResult struct {
	Listed       int      `json:"listed"`               // topics returned by the Kafka listing; 0 when no listing ran
	Started      []string `json:"started"`              // tasks launched this tick (new matches and recovered table_missing topics)
	TableMissing []string `json:"table_missing"`        // new matches parked because their table does not exist yet
	Removed      []string `json:"removed"`              // pattern topics forgotten because they disappeared from Kafka
	ListError    string   `json:"list_error,omitempty"` // why the listing failed; pattern reconciliation was skipped
}

// Reconcile runs one discovery tick:
//
//  1. list the cluster's topics and expand topic_patterns into concrete
//     mappings, skipping internal and DLQ topics, topics with an explicit
//     topic_tables entry, and topics already managed;
//  2. start a sink task for every new match whose table exists, or park it as
//     table_missing;
//  3. forget pattern topics absent from discoveryDeleteAfterMisses consecutive
//     listings (their task is stopped and removed);
//  4. retry every parked table_missing topic, explicit or discovered.
//
// Network calls (Kafka metadata, ClickHouse DESCRIBE) run outside m.mu so the
// admin API and health checks never block on them. Only one tick runs at a
// time. A listing failure skips steps 1 to 3 for that tick, so a Kafka blip can
// never remove a task.
func (m *TaskManager) Reconcile(ctx context.Context) DiscoveryResult {
	m.discoveryMu.Lock()
	defer m.discoveryMu.Unlock()

	res := DiscoveryResult{Started: []string{}, TableMissing: []string{}, Removed: []string{}}
	if m.lister != nil && len(m.cfg.TopicPatterns) > 0 {
		listCtx, cancel := context.WithTimeout(ctx, m.discoveryTimeout())
		topics, err := m.lister.ListTopics(listCtx)
		cancel()
		if err != nil {
			discoveryErrorsTotal.Inc()
			res.ListError = err.Error()
			m.logger.Warnf("discovery: listing Kafka topics failed, skipping pattern reconciliation this tick: %v", err)
		} else {
			res.Listed = len(topics)
			m.reconcilePatterns(ctx, topics, &res)
		}
	}
	m.retryMissingTables(ctx, &res)
	m.updateDiscoveryGauges()
	return res
}

// RunDiscovery calls Reconcile every interval until ctx is cancelled. The first
// tick is run synchronously by the caller at startup so pattern-matched topics
// start alongside the explicit ones.
func (m *TaskManager) RunDiscovery(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.Reconcile(ctx)
		}
	}
}

func (m *TaskManager) discoveryTimeout() time.Duration {
	if m.cfg == nil || m.cfg.TopicDiscovery.TimeoutMs <= 0 {
		return 5 * time.Second
	}
	return time.Duration(m.cfg.TopicDiscovery.TimeoutMs) * time.Millisecond
}

// reconcilePatterns expands the listing into new tasks and garbage-collects
// pattern tasks whose topic is gone.
func (m *TaskManager) reconcilePatterns(ctx context.Context, topics []string, res *DiscoveryResult) {
	explicit := make(map[string]struct{}, len(m.cfg.TopicTables))
	for _, tt := range m.cfg.TopicTables {
		explicit[tt.Topic] = struct{}{}
	}
	present := make(map[string]struct{}, len(topics))

	// Decide what is new under the lock; no I/O happens here.
	var candidates []TopicTableMapping
	m.mu.Lock()
	for _, topic := range topics {
		present[topic] = struct{}{}
		if isExcludedTopic(topic, m.cfg.DLQTopicSuffix) {
			continue
		}
		if _, ok := explicit[topic]; ok {
			continue
		}
		if _, ok := m.tasks[topic]; ok {
			continue
		}
		if _, ok := m.skippedTables[topic]; ok {
			continue
		}
		if mapping, ok := m.matchPatternLocked(topic); ok {
			candidates = append(candidates, mapping)
		}
	}
	m.mu.Unlock()

	for _, mapping := range candidates {
		if ctx.Err() != nil || m.parentCtx.Err() != nil {
			return
		}
		if !m.patternCapacityLeft() {
			m.logger.Warnf("discovery: topic_discovery.max_topics (%d) reached; not starting %s or any other new matching topic", m.cfg.TopicDiscovery.MaxTopics, mapping.Topic)
			break
		}
		cols, err := m.describeWithTimeout(ctx, mapping)
		if err != nil {
			m.mu.Lock()
			m.skippedTables[mapping.Topic] = mapping
			m.mu.Unlock()
			res.TableMissing = append(res.TableMissing, mapping.Topic)
			m.logger.Warnf("discovery: topic %s matched pattern %q but table %q did not validate: %v; parked as table_missing and retried every tick", mapping.Topic, mapping.SourcePattern, mapping.Table, err)
			continue
		}
		m.logger.Infof("discovery: topic %s matched pattern %q -> table %s", mapping.Topic, mapping.SourcePattern, mapping.Table)
		m.launch(mapping, cols, res)
	}

	m.collectDeleted(present, res)
}

// matchPatternLocked expands topic against topic_patterns in config order; the
// first match wins. A topic matching several patterns is logged once. The
// caller holds m.mu.
func (m *TaskManager) matchPatternLocked(topic string) (TopicTableMapping, bool) {
	var winner TopicTableMapping
	var extra []string
	matched := false
	for i := range m.cfg.TopicPatterns {
		p := &m.cfg.TopicPatterns[i]
		mapping, ok := p.expand(topic)
		if !ok {
			continue
		}
		if !matched {
			winner, matched = mapping, true
			continue
		}
		extra = append(extra, p.Pattern)
	}
	if len(extra) > 0 {
		if _, warned := m.multiMatchWarned[topic]; !warned {
			m.multiMatchWarned[topic] = struct{}{}
			m.logger.Warnf("discovery: topic %s matches several patterns; using %q, ignoring %q", topic, winner.SourcePattern, extra)
		}
	}
	return winner, matched
}

// patternCapacityLeft reports whether another pattern-discovered task may start.
// Running and stopped tasks both count; parked table_missing topics do not.
func (m *TaskManager) patternCapacityLeft() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := 0
	for _, mt := range m.tasks {
		if mt.mapping.SourcePattern != "" {
			n++
		}
	}
	return n < m.cfg.TopicDiscovery.MaxTopics
}

// describeWithTimeout fetches the mapping's table columns, treating a table
// without columns as missing and a table without the tombstone columns as not
// valid (as validateTables does).
func (m *TaskManager) describeWithTimeout(ctx context.Context, mapping TopicTableMapping) (ColumnInfo, error) {
	descCtx, cancel := context.WithTimeout(ctx, m.discoveryTimeout())
	defer cancel()
	cols, err := m.describeTable(descCtx, mapping.Table)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("table %q has no columns or does not exist", mapping.Table)
	}
	if err := validateTombstoneColumns(mapping, cols); err != nil {
		return nil, err
	}
	return cols, nil
}

// launch records the table's columns and starts the task. It returns false when
// the task could not be started; the error is logged and the caller decides how
// the topic is retried.
func (m *TaskManager) launch(mapping TopicTableMapping, cols ColumnInfo, res *DiscoveryResult) bool {
	if m.parentCtx.Err() != nil {
		return false
	}
	m.mu.Lock()
	m.tableColumns[mapping.Table] = cols
	m.mu.Unlock()
	if err := m.startTaskFn(mapping); err != nil {
		m.logger.Errorf("discovery: failed to start sink task for topic %s: %v", mapping.Topic, err)
		return false
	}
	res.Started = append(res.Started, mapping.Topic)
	return true
}

// retryMissingTables re-validates every parked topic and starts those whose
// table now exists. Explicit and discovered topics are treated alike.
func (m *TaskManager) retryMissingTables(ctx context.Context, res *DiscoveryResult) {
	m.mu.RLock()
	parked := make([]TopicTableMapping, 0, len(m.skippedTables))
	for _, mapping := range m.skippedTables {
		parked = append(parked, mapping)
	}
	m.mu.RUnlock()
	sort.Slice(parked, func(i, j int) bool { return parked[i].Topic < parked[j].Topic })

	for _, mapping := range parked {
		if ctx.Err() != nil || m.parentCtx.Err() != nil {
			return
		}
		cols, err := m.describeWithTimeout(ctx, mapping)
		if err != nil {
			continue // still missing; stays parked without re-logging every tick
		}
		if mapping.SourcePattern != "" && !m.patternCapacityLeft() {
			m.logger.Warnf("discovery: table %s for topic %s is available but topic_discovery.max_topics (%d) is reached; leaving it parked", mapping.Table, mapping.Topic, m.cfg.TopicDiscovery.MaxTopics)
			continue
		}

		// Unpark before launching so the topic is never listed twice; a failed
		// launch parks it again so it stays visible and is retried next tick.
		m.mu.Lock()
		_, still := m.skippedTables[mapping.Topic]
		delete(m.skippedTables, mapping.Topic)
		m.mu.Unlock()
		if !still {
			continue // forgotten meanwhile (its topic was deleted)
		}
		m.logger.Infof("discovery: table %s for topic %s is now available, starting sink task", mapping.Table, mapping.Topic)
		if !m.launch(mapping, cols, res) {
			m.mu.Lock()
			m.skippedTables[mapping.Topic] = mapping
			m.mu.Unlock()
		}
	}
}

// collectDeleted forgets pattern topics that are gone from Kafka. Parked
// (table_missing) topics are dropped immediately, since they hold no consumer;
// running or stopped tasks are removed only after discoveryDeleteAfterMisses
// consecutive absences. Explicit topic_tables entries are never touched. The
// caller must not hold m.mu.
func (m *TaskManager) collectDeleted(present map[string]struct{}, res *DiscoveryResult) {
	var gone []*managedTask
	m.mu.Lock()
	for topic, mt := range m.tasks {
		if mt.mapping.SourcePattern == "" {
			continue
		}
		if _, ok := present[topic]; ok {
			delete(m.patternMisses, topic)
			continue
		}
		m.patternMisses[topic]++
		if m.patternMisses[topic] < discoveryDeleteAfterMisses {
			m.logger.Warnf("discovery: topic %s is missing from Kafka metadata (%d/%d); its task is removed if still missing next tick", topic, m.patternMisses[topic], discoveryDeleteAfterMisses)
			continue
		}
		gone = append(gone, mt)
	}
	for topic, mapping := range m.skippedTables {
		if mapping.SourcePattern == "" {
			continue
		}
		if _, ok := present[topic]; !ok {
			delete(m.skippedTables, topic)
			res.Removed = append(res.Removed, topic)
			m.logger.Infof("discovery: parked topic %s is gone from Kafka; forgetting it", topic)
		}
	}
	m.mu.Unlock()

	for _, mt := range gone {
		m.removeTask(mt)
		res.Removed = append(res.Removed, mt.mapping.Topic)
	}
}

// removeTask stops a pattern-discovered task whose topic was deleted and forgets
// it; the next tick respawns it if the topic comes back. The stop is marked as
// operator-initiated so neither the supervisor nor /livez react to it, and the
// task_stopped series is dropped so a stale "stopped" gauge cannot page anyone.
func (m *TaskManager) removeTask(mt *managedTask) {
	topic := mt.mapping.Topic
	mt.task.MarkOperatorStop()
	mt.cancel()
	<-mt.done

	m.mu.Lock()
	if cur, ok := m.tasks[topic]; ok && cur == mt {
		delete(m.tasks, topic)
	}
	delete(m.restart, topic)
	delete(m.patternMisses, topic)
	m.mu.Unlock()

	taskStopped.DeleteLabelValues(topic)
	discoveryTopicsRemovedTotal.WithLabelValues(mt.mapping.SourcePattern).Inc()
	m.logger.Warnf("discovery: topic %s was deleted from Kafka; removed its sink task (pattern %q)", topic, mt.mapping.SourcePattern)
}

// updateDiscoveryGauges publishes how many topics each pattern currently owns,
// counting running and stopped tasks as well as parked table_missing topics.
func (m *TaskManager) updateDiscoveryGauges() {
	counts := make(map[string]int, len(m.cfg.TopicPatterns))
	for _, p := range m.cfg.TopicPatterns {
		counts[p.Pattern] = 0
	}
	m.mu.RLock()
	for _, mt := range m.tasks {
		if mt.mapping.SourcePattern != "" {
			counts[mt.mapping.SourcePattern]++
		}
	}
	for _, mapping := range m.skippedTables {
		if mapping.SourcePattern != "" {
			counts[mapping.SourcePattern]++
		}
	}
	m.mu.RUnlock()
	for pattern, n := range counts {
		discoveredTopics.WithLabelValues(pattern).Set(float64(n))
	}
}

// isExcludedTopic filters topics that patterns must never pick up: Kafka and
// Confluent internal topics (__consumer_offsets, _schemas, _confluent-*) and
// kahouse's own dead-letter topics.
func isExcludedTopic(topic, dlqSuffix string) bool {
	if strings.HasPrefix(topic, "_") {
		return true
	}
	return dlqSuffix != "" && strings.HasSuffix(topic, dlqSuffix)
}

// handleRunDiscovery triggers a reconcile tick on demand, so an operator does not
// have to wait for the next interval after creating a topic or table.
func (m *TaskManager) handleRunDiscovery(w http.ResponseWriter, _ *http.Request) {
	// The tick runs on the process context, not the request's, so a client that
	// disconnects mid-tick does not abort task launches halfway through.
	res := m.Reconcile(m.parentCtx)
	w.Header().Set("Content-Type", "application/json")
	if res.ListError != "" {
		w.WriteHeader(http.StatusBadGateway)
	}
	if err := json.NewEncoder(w).Encode(res); err != nil {
		m.logger.Errorf("Failed to encode discovery response: %v", err)
	}
}
