package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
)

// fakeLister serves a mutable topic listing, or an error, to Reconcile.
type fakeLister struct {
	mu     sync.Mutex
	topics []string
	err    error
}

func (f *fakeLister) ListTopics(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return append([]string(nil), f.topics...), nil
}

func (f *fakeLister) set(topics ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topics = topics
	f.err = nil
}

func (f *fakeLister) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// fakeTables is the set of ClickHouse tables that exist, as seen by describeTable.
type fakeTables struct {
	mu     sync.Mutex
	tables map[string]ColumnInfo
}

func (f *fakeTables) describe(_ context.Context, table string) (ColumnInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cols, ok := f.tables[table]
	if !ok {
		return nil, errors.New("table does not exist")
	}
	return cols, nil
}

func (f *fakeTables) create(table string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tables[table] = ColumnInfo{"id": "Int64"}
}

// discoveryFixture wires a TaskManager to a fake topic listing and fake tables.
// Tasks are started through an in-memory stand-in whose goroutine exits on
// cancel, so no Kafka or ClickHouse is touched.
type discoveryFixture struct {
	mgr    *TaskManager
	lister *fakeLister
	tables *fakeTables
	cancel context.CancelFunc
}

func newDiscoveryFixture(t *testing.T, cfg *Config, patterns ...TopicPattern) *discoveryFixture {
	t.Helper()
	if cfg == nil {
		cfg = &Config{InputFormat: "json"}
	}
	cfg.TopicPatterns = patterns
	applyDefaults(cfg)
	for i := range cfg.TopicTables {
		cfg.TopicTables[i].resolve(cfg)
	}
	for i := range cfg.TopicPatterns {
		cfg.TopicPatterns[i].resolve(cfg)
		if err := cfg.TopicPatterns[i].compile(); err != nil {
			t.Fatalf("compile pattern %q: %v", cfg.TopicPatterns[i].Pattern, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f := &discoveryFixture{
		lister: &fakeLister{},
		tables: &fakeTables{tables: make(map[string]ColumnInfo)},
		cancel: cancel,
	}
	f.mgr = &TaskManager{
		tasks:            make(map[string]*managedTask),
		restart:          make(map[string]*restartTracker),
		cfg:              cfg,
		logger:           zap.NewNop().Sugar(),
		parentCtx:        ctx,
		tableColumns:     make(TableColumns),
		skippedTables:    make(map[string]TopicTableMapping),
		lister:           f.lister,
		patternMisses:    make(map[string]int),
		multiMatchWarned: make(map[string]struct{}),
	}
	f.mgr.describeTable = f.tables.describe
	f.mgr.startTaskFn = func(mapping TopicTableMapping) error {
		task := &SinkTask{mapping: mapping, startedAt: time.Now()}
		taskCtx, taskCancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			<-taskCtx.Done()
			task.stopped.Store(true)
			close(done)
		}()
		f.mgr.mu.Lock()
		f.mgr.tasks[mapping.Topic] = &managedTask{task: task, cancel: taskCancel, done: done, mapping: mapping, startedAt: time.Now()}
		f.mgr.mu.Unlock()
		return nil
	}
	return f
}

func (f *discoveryFixture) taskTopics() []string {
	f.mgr.mu.RLock()
	defer f.mgr.mu.RUnlock()
	out := make([]string, 0, len(f.mgr.tasks))
	for topic := range f.mgr.tasks {
		out = append(out, topic)
	}
	sort.Strings(out)
	return out
}

func (f *discoveryFixture) status(t *testing.T, topic string) TopicStatus {
	t.Helper()
	for _, ts := range f.mgr.Topics() {
		if ts.Topic == topic {
			return ts
		}
	}
	t.Fatalf("topic %q not in Topics(): %+v", topic, f.mgr.Topics())
	return TopicStatus{}
}

func hrPattern() TopicPattern {
	return TopicPattern{
		Pattern:           `prod\.(?P<domain>[a-z]+)\.(?P<entity>[a-z_]+)`,
		TopicTableMapping: TopicTableMapping{Table: "analytics.${domain}_${entity}"},
	}
}

func assertStrings(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func TestReconcileStartsMatchingTopics(t *testing.T) {
	f := newDiscoveryFixture(t, nil, hrPattern())
	f.lister.set("prod.hr.employees", "prod.hr.leaves", "staging.hr.employees", "_schemas", "__consumer_offsets", "prod.hr.employees.dlq")
	f.tables.create("analytics.hr_employees")
	f.tables.create("analytics.hr_leaves")

	res := f.mgr.Reconcile(context.Background())
	if res.Listed != 6 {
		t.Fatalf("Listed = %d, want 6", res.Listed)
	}
	assertStrings(t, "Started", res.Started, []string{"prod.hr.employees", "prod.hr.leaves"})
	assertStrings(t, "tasks", f.taskTopics(), []string{"prod.hr.employees", "prod.hr.leaves"})
	if res.ListError != "" || len(res.TableMissing) != 0 || len(res.Removed) != 0 {
		t.Fatalf("Unexpected result: %+v", res)
	}

	st := f.status(t, "prod.hr.employees")
	if st.Source != "pattern" || st.Pattern != hrPattern().Pattern || st.Table != "analytics.hr_employees" || st.Status != "running" {
		t.Fatalf("Unexpected status: %+v", st)
	}
	f.mgr.mu.RLock()
	_, registered := f.mgr.tableColumns["analytics.hr_employees"]
	f.mgr.mu.RUnlock()
	if !registered {
		t.Fatal("Expected the discovered table's columns to be registered for the sink task")
	}
	if got := testutil.ToFloat64(discoveredTopics.WithLabelValues(hrPattern().Pattern)); got != 2 {
		t.Fatalf("discovered_topics gauge = %v, want 2", got)
	}

	// A second tick with the same listing changes nothing.
	res = f.mgr.Reconcile(context.Background())
	if len(res.Started) != 0 || len(res.Removed) != 0 {
		t.Fatalf("Expected an idempotent second tick, got %+v", res)
	}
	assertStrings(t, "tasks after second tick", f.taskTopics(), []string{"prod.hr.employees", "prod.hr.leaves"})
}

func TestReconcileExplicitMappingWins(t *testing.T) {
	cfg := &Config{
		InputFormat: "json",
		TopicTables: []TopicTableMapping{{Topic: "prod.hr.employees", Table: "custom.employees"}},
	}
	f := newDiscoveryFixture(t, cfg, hrPattern())
	f.lister.set("prod.hr.employees", "prod.hr.leaves")
	f.tables.create("analytics.hr_employees")
	f.tables.create("analytics.hr_leaves")

	res := f.mgr.Reconcile(context.Background())
	// The explicit topic belongs to StartAll (not called here), so discovery must
	// neither start nor park it.
	assertStrings(t, "Started", res.Started, []string{"prod.hr.leaves"})
	assertStrings(t, "tasks", f.taskTopics(), []string{"prod.hr.leaves"})
	if len(f.mgr.skippedTables) != 0 {
		t.Fatalf("Expected no parked topics, got %v", f.mgr.skippedTables)
	}
}

func TestReconcileFirstPatternWins(t *testing.T) {
	first := hrPattern()
	second := TopicPattern{Pattern: `prod\..*`, TopicTableMapping: TopicTableMapping{Table: "catchall.${0}"}}
	f := newDiscoveryFixture(t, nil, first, second)
	f.lister.set("prod.hr.employees", "prod.other")
	f.tables.create("analytics.hr_employees")
	f.tables.create("catchall.prod.other")

	f.mgr.Reconcile(context.Background())

	if st := f.status(t, "prod.hr.employees"); st.Table != "analytics.hr_employees" || st.Pattern != first.Pattern {
		t.Fatalf("Expected the first pattern to win, got %+v", st)
	}
	if st := f.status(t, "prod.other"); st.Table != "catchall.prod.other" || st.Pattern != second.Pattern {
		t.Fatalf("Expected the second pattern to catch prod.other, got %+v", st)
	}
	if got := testutil.ToFloat64(discoveredTopics.WithLabelValues(first.Pattern)); got != 1 {
		t.Fatalf("discovered_topics{first} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(discoveredTopics.WithLabelValues(second.Pattern)); got != 1 {
		t.Fatalf("discovered_topics{second} = %v, want 1", got)
	}
}

func TestReconcileParksMissingTableThenStarts(t *testing.T) {
	f := newDiscoveryFixture(t, nil, hrPattern())
	f.lister.set("prod.hr.employees")

	res := f.mgr.Reconcile(context.Background())
	assertStrings(t, "TableMissing", res.TableMissing, []string{"prod.hr.employees"})
	assertStrings(t, "Started", res.Started, nil)

	st := f.status(t, "prod.hr.employees")
	if st.Status != "stopped" || st.StopReason != "table_missing" || st.Source != "pattern" || st.Table != "analytics.hr_employees" {
		t.Fatalf("Expected stopped/table_missing/pattern, got %+v", st)
	}
	if len(f.mgr.Snapshot()) != 0 {
		t.Fatal("A parked topic must not appear in the health snapshot")
	}
	if got := testutil.ToFloat64(discoveredTopics.WithLabelValues(hrPattern().Pattern)); got != 1 {
		t.Fatalf("discovered_topics gauge = %v, want 1 (parked topics count)", got)
	}

	// Still missing: parked once, not reported as new again.
	res = f.mgr.Reconcile(context.Background())
	if len(res.TableMissing) != 0 || len(res.Started) != 0 {
		t.Fatalf("Expected a quiet tick while the table is missing, got %+v", res)
	}

	f.tables.create("analytics.hr_employees")
	res = f.mgr.Reconcile(context.Background())
	assertStrings(t, "Started", res.Started, []string{"prod.hr.employees"})
	if st := f.status(t, "prod.hr.employees"); st.Status != "running" {
		t.Fatalf("Expected the topic to start once its table exists, got %+v", st)
	}
	if len(f.mgr.skippedTables) != 0 {
		t.Fatalf("Expected no parked topics after start, got %v", f.mgr.skippedTables)
	}
}

func TestReconcileRetriesExplicitMissingTables(t *testing.T) {
	cfg := &Config{
		InputFormat: "json",
		TopicTables: []TopicTableMapping{{Topic: "orders", Table: "default.orders"}},
	}
	f := newDiscoveryFixture(t, cfg)                   // no patterns: the tick only retries parked tables
	f.mgr.skippedTables["orders"] = cfg.TopicTables[0] // as StartAll parks it

	res := f.mgr.Reconcile(context.Background())
	if res.Listed != 0 || len(res.Started) != 0 {
		t.Fatalf("Expected no listing and no start while the table is missing, got %+v", res)
	}
	if st := f.status(t, "orders"); st.StopReason != "table_missing" || st.Source != "config" {
		t.Fatalf("Expected orders parked as config/table_missing, got %+v", st)
	}

	f.tables.create("default.orders")
	res = f.mgr.Reconcile(context.Background())
	assertStrings(t, "Started", res.Started, []string{"orders"})
	if st := f.status(t, "orders"); st.Status != "running" || st.Source != "config" {
		t.Fatalf("Expected orders running from config, got %+v", st)
	}
}

func TestReconcileRespectsMaxTopics(t *testing.T) {
	cfg := &Config{InputFormat: "json", TopicDiscovery: TopicDiscoveryConfig{MaxTopics: 1}}
	f := newDiscoveryFixture(t, cfg, hrPattern())
	f.lister.set("prod.hr.a", "prod.hr.b")
	f.tables.create("analytics.hr_a")
	f.tables.create("analytics.hr_b")

	res := f.mgr.Reconcile(context.Background())
	assertStrings(t, "Started", res.Started, []string{"prod.hr.a"})
	assertStrings(t, "tasks", f.taskTopics(), []string{"prod.hr.a"})

	res = f.mgr.Reconcile(context.Background())
	if len(res.Started) != 0 {
		t.Fatalf("Expected the cap to hold on later ticks, got %+v", res)
	}
}

func TestReconcileRemovesDeletedTopicAfterTwoMisses(t *testing.T) {
	pattern := hrPattern().Pattern
	f := newDiscoveryFixture(t, nil, hrPattern())
	f.lister.set("prod.hr.employees")
	f.tables.create("analytics.hr_employees")
	f.mgr.Reconcile(context.Background())

	f.mgr.mu.RLock()
	mt := f.mgr.tasks["prod.hr.employees"]
	f.mgr.mu.RUnlock()
	f.mgr.restart["prod.hr.employees"] = &restartTracker{failures: 1}
	taskStopped.WithLabelValues("prod.hr.employees").Set(0)
	removedBefore := testutil.ToFloat64(discoveryTopicsRemovedTotal.WithLabelValues(pattern))

	// First miss: warn only, keep the task.
	f.lister.set()
	res := f.mgr.Reconcile(context.Background())
	assertStrings(t, "Removed after one miss", res.Removed, nil)
	assertStrings(t, "tasks after one miss", f.taskTopics(), []string{"prod.hr.employees"})
	if f.mgr.patternMisses["prod.hr.employees"] != 1 {
		t.Fatalf("patternMisses = %d, want 1", f.mgr.patternMisses["prod.hr.employees"])
	}

	// Second consecutive miss: stop, forget, clean up.
	res = f.mgr.Reconcile(context.Background())
	assertStrings(t, "Removed after two misses", res.Removed, []string{"prod.hr.employees"})
	assertStrings(t, "tasks after two misses", f.taskTopics(), nil)
	if !mt.task.IsStopped() || !mt.task.StoppedByOperator() {
		t.Fatal("Expected the removed task to be stopped and flagged as an operator stop")
	}
	if _, ok := f.mgr.restart["prod.hr.employees"]; ok {
		t.Fatal("Expected the supervisor's restart tracker to be dropped")
	}
	if _, ok := f.mgr.patternMisses["prod.hr.employees"]; ok {
		t.Fatal("Expected the miss counter to be dropped")
	}
	if taskStopped.DeleteLabelValues("prod.hr.employees") {
		t.Fatal("Expected the task_stopped series to have been deleted on removal")
	}
	if got := testutil.ToFloat64(discoveryTopicsRemovedTotal.WithLabelValues(pattern)); got != removedBefore+1 {
		t.Fatalf("discovery_topics_removed_total = %v, want %v", got, removedBefore+1)
	}
	if got := testutil.ToFloat64(discoveredTopics.WithLabelValues(pattern)); got != 0 {
		t.Fatalf("discovered_topics gauge = %v, want 0", got)
	}

	// The topic comes back: a fresh task is spawned.
	f.lister.set("prod.hr.employees")
	res = f.mgr.Reconcile(context.Background())
	assertStrings(t, "Started after re-create", res.Started, []string{"prod.hr.employees"})
	f.mgr.mu.RLock()
	fresh := f.mgr.tasks["prod.hr.employees"]
	f.mgr.mu.RUnlock()
	if fresh == nil || fresh == mt {
		t.Fatal("Expected a new task incarnation after the topic was re-created")
	}
}

func TestReconcileForgetsParkedTopicWhenDeleted(t *testing.T) {
	f := newDiscoveryFixture(t, nil, hrPattern())
	f.lister.set("prod.hr.employees") // no table: parked
	f.mgr.Reconcile(context.Background())
	if len(f.mgr.skippedTables) != 1 {
		t.Fatalf("Expected one parked topic, got %v", f.mgr.skippedTables)
	}

	// A parked topic holds no consumer, so it is forgotten on the first miss.
	f.lister.set()
	res := f.mgr.Reconcile(context.Background())
	assertStrings(t, "Removed", res.Removed, []string{"prod.hr.employees"})
	if len(f.mgr.skippedTables) != 0 {
		t.Fatalf("Expected the parked topic to be forgotten, got %v", f.mgr.skippedTables)
	}
}

func TestReconcileMissCounterResetsWhenTopicReappears(t *testing.T) {
	f := newDiscoveryFixture(t, nil, hrPattern())
	f.lister.set("prod.hr.employees")
	f.tables.create("analytics.hr_employees")
	f.mgr.Reconcile(context.Background())

	f.lister.set()
	f.mgr.Reconcile(context.Background()) // miss 1
	f.lister.set("prod.hr.employees")
	f.mgr.Reconcile(context.Background()) // back: counter resets
	f.lister.set()
	f.mgr.Reconcile(context.Background()) // miss 1 again, not 2

	assertStrings(t, "tasks", f.taskTopics(), []string{"prod.hr.employees"})
	if f.mgr.patternMisses["prod.hr.employees"] != 1 {
		t.Fatalf("patternMisses = %d, want 1 after a flap", f.mgr.patternMisses["prod.hr.employees"])
	}
}

func TestReconcileListingErrorNeverRemovesTasks(t *testing.T) {
	f := newDiscoveryFixture(t, nil, hrPattern())
	f.lister.set("prod.hr.employees")
	f.tables.create("analytics.hr_employees")
	f.mgr.Reconcile(context.Background())
	errorsBefore := testutil.ToFloat64(discoveryErrorsTotal)

	f.lister.fail(errors.New("Local: Broker transport failure"))
	for i := 0; i < 3; i++ {
		res := f.mgr.Reconcile(context.Background())
		if !strings.Contains(res.ListError, "Broker transport failure") || res.Listed != 0 {
			t.Fatalf("Expected the listing error to be reported, got %+v", res)
		}
		if len(res.Removed) != 0 {
			t.Fatalf("A failed listing must never remove tasks, got %+v", res)
		}
	}
	assertStrings(t, "tasks", f.taskTopics(), []string{"prod.hr.employees"})
	if len(f.mgr.patternMisses) != 0 {
		t.Fatalf("A failed listing must not count as a miss, got %v", f.mgr.patternMisses)
	}
	if got := testutil.ToFloat64(discoveryErrorsTotal); got != errorsBefore+3 {
		t.Fatalf("discovery_errors_total = %v, want %v", got, errorsBefore+3)
	}
}

func TestReconcileNeverRemovesExplicitTasks(t *testing.T) {
	cfg := &Config{
		InputFormat: "json",
		TopicTables: []TopicTableMapping{{Topic: "orders", Table: "default.orders"}},
	}
	f := newDiscoveryFixture(t, cfg, hrPattern())
	if err := f.mgr.startTaskFn(cfg.TopicTables[0]); err != nil {
		t.Fatal(err)
	}

	f.lister.set() // orders is not in Kafka at all
	for i := 0; i < 3; i++ {
		if res := f.mgr.Reconcile(context.Background()); len(res.Removed) != 0 {
			t.Fatalf("Explicit topics must never be removed, got %+v", res)
		}
	}
	assertStrings(t, "tasks", f.taskTopics(), []string{"orders"})
	if st := f.status(t, "orders"); st.Source != "config" || st.Status != "running" {
		t.Fatalf("Unexpected status for explicit topic: %+v", st)
	}
}

func TestReconcileDoesNotRespawnOperatorStoppedTopic(t *testing.T) {
	f := newDiscoveryFixture(t, nil, hrPattern())
	f.lister.set("prod.hr.employees")
	f.tables.create("analytics.hr_employees")
	f.mgr.Reconcile(context.Background())

	if err := f.mgr.Stop("prod.hr.employees"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	f.mgr.mu.RLock()
	stopped := f.mgr.tasks["prod.hr.employees"]
	f.mgr.mu.RUnlock()

	res := f.mgr.Reconcile(context.Background())
	assertStrings(t, "Started", res.Started, nil)
	f.mgr.mu.RLock()
	cur := f.mgr.tasks["prod.hr.employees"]
	f.mgr.mu.RUnlock()
	if cur != stopped {
		t.Fatal("Discovery must leave an operator-stopped topic alone")
	}
	if st := f.status(t, "prod.hr.employees"); st.Status != "stopped" || st.StopReason != "operator" {
		t.Fatalf("Expected stopped/operator, got %+v", st)
	}
	// It still counts against max_topics and in the gauge.
	if got := testutil.ToFloat64(discoveredTopics.WithLabelValues(hrPattern().Pattern)); got != 1 {
		t.Fatalf("discovered_topics gauge = %v, want 1", got)
	}
}

func TestReconcileSkipsLaunchesDuringShutdown(t *testing.T) {
	f := newDiscoveryFixture(t, nil, hrPattern())
	f.lister.set("prod.hr.employees")
	f.tables.create("analytics.hr_employees")
	f.cancel() // process is shutting down

	res := f.mgr.Reconcile(context.Background())
	assertStrings(t, "Started", res.Started, nil)
	assertStrings(t, "tasks", f.taskTopics(), nil)
}

func TestHandleRunDiscoveryEndpoint(t *testing.T) {
	f := newDiscoveryFixture(t, nil, hrPattern())
	f.lister.set("prod.hr.employees")
	f.tables.create("analytics.hr_employees")

	mux := http.NewServeMux()
	RegisterAdminEndpoints(f.mgr, mux)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("POST", "/api/discovery/run", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d (body: %s)", rr.Code, rr.Body.String())
	}
	var res DiscoveryResult
	if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	assertStrings(t, "Started", res.Started, []string{"prod.hr.employees"})
	if res.Listed != 1 || res.ListError != "" {
		t.Fatalf("Unexpected result: %+v", res)
	}

	f.lister.fail(errors.New("broker down"))
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("POST", "/api/discovery/run", nil))
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("Expected 502 on a listing failure, got %d (body: %s)", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"list_error":"broker down"`) || !strings.Contains(body, `"started":[]`) {
		t.Fatalf("Unexpected body: %s", body)
	}
}

func TestIsExcludedTopic(t *testing.T) {
	tests := []struct {
		topic string
		want  bool
	}{
		{"__consumer_offsets", true},
		{"_schemas", true},
		{"_confluent-command", true},
		{"orders.dlq", true},
		{"orders", false},
		{"prod.hr.employees", false},
		{"my_topic", false},
		{"dlq", false},
	}
	for _, tt := range tests {
		if got := isExcludedTopic(tt.topic, ".dlq"); got != tt.want {
			t.Errorf("isExcludedTopic(%q) = %v, want %v", tt.topic, got, tt.want)
		}
	}
	if isExcludedTopic("orders.dlq", "") {
		t.Error("An empty DLQ suffix must not exclude anything by suffix")
	}
}
