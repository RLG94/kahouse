package app

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

// All metrics are namespaced under "kahouse" to avoid collisions with other applications.
var (
	msgConsumed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "msg_consumed_total",
		Help:      "Total number of messages consumed from Kafka",
	}, []string{"topic"})
	msgProduced = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "msg_produced_total",
		Help:      "Total number of messages written to ClickHouse",
	}, []string{"topic"})
	msgFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "msg_failed_total",
		Help:      "Total number of messages that failed processing (deserialization errors and batch write failures)",
	}, []string{"topic"})
	msgDLQ = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "msg_dlq_total",
		Help:      "Total number of messages sent to the dead letter queue",
	}, []string{"topic"})
	msgTombstones = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "msg_tombstones_total",
		Help:      "Total number of tombstones written as deletion rows (tombstones.mode: row)",
	}, []string{"topic"})
	batchSizeHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kahouse",
		Name:      "batch_size",
		Help:      "Number of messages per flushed batch",
		Buckets:   prometheus.ExponentialBuckets(10, 2, 10),
	}, []string{"topic"})
	batchDelayHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kahouse",
		Name:      "batch_delay_seconds",
		Help:      "Wall-clock time from the first message's arrival in the batch to flush. Not the message's age since it was produced: see record_receive_lag_seconds",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 10),
	}, []string{"topic"})
	processLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kahouse",
		Name:      "process_latency_seconds",
		Help:      "Time spent writing a batch to ClickHouse (includes retry backoff)",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 10),
	}, []string{"topic"})
	// A summary without quantiles exports only _sum and _count: the mean over
	// any window is sum/count, at two series per topic.
	recordReceiveLag = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "kahouse",
		Name:      "record_receive_lag_seconds",
		Help:      "Time from a message's Kafka timestamp to its read by the sink task",
	}, []string{"topic"})
	retryCountHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kahouse",
		Name:      "write_retry_count",
		Help:      "Number of retry attempts per ClickHouse batch write",
		Buckets:   prometheus.LinearBuckets(0, 1, 11),
	}, []string{"topic"})
	taskStopped = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kahouse",
		Name:      "task_stopped",
		Help:      "Whether a sink task has stopped (1 = stopped, 0 = running)",
	}, []string{"topic"})
	taskRestartsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "task_restarts_total",
		Help:      "Total number of times a sink task has been restarted via the admin API",
	}, []string{"topic"})
	taskAutoRestartsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "task_auto_restarts_total",
		Help:      "Total number of times the supervisor automatically restarted a transiently-crashed sink task",
	}, []string{"topic"})
	taskRecycleEscalationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "task_recycle_escalations_total",
		Help:      "Total number of times the supervisor gave up auto-restarting a task and escalated to a pod recycle via liveness",
	}, []string{"topic"})
	startAtAppliedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "start_at_applied_total",
		Help:      "Per-partition starting position decisions made on assignment. Labels: topic, decision (committed | start_at_offset | start_at_timestamp | start_at_position | fallback_auto_reset).",
	}, []string{"topic", "decision"})
	discoveredTopics = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kahouse",
		Name:      "discovered_topics",
		Help:      "Topics currently managed from a topic_patterns entry (running, stopped, or parked as table_missing)",
	}, []string{"pattern"})
	discoveryErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "discovery_errors_total",
		Help:      "Discovery ticks whose Kafka topic listing failed; pattern reconciliation is skipped for that tick",
	})
	discoveryTopicsRemovedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "discovery_topics_removed_total",
		Help:      "Pattern-discovered sink tasks removed because their topic disappeared from Kafka",
	}, []string{"pattern"})
)

func init() {
	prometheus.MustRegister(
		msgConsumed,
		msgProduced,
		msgFailed,
		msgDLQ,
		msgTombstones,
		batchSizeHist,
		batchDelayHist,
		processLatency,
		recordReceiveLag,
		retryCountHist,
		taskStopped,
		taskRestartsTotal,
		taskAutoRestartsTotal,
		taskRecycleEscalationsTotal,
		startAtAppliedTotal,
		discoveredTopics,
		discoveryErrorsTotal,
		discoveryTopicsRemovedTotal,
	)
}

// observeReceiveLag records how long after its Kafka timestamp a message was
// read. Messages without a timestamp are skipped.
func observeReceiveLag(topic string, msg *kafka.Message, now time.Time) {
	if msg.TimestampType == kafka.TimestampNotAvailable {
		return
	}
	recordReceiveLag.WithLabelValues(topic).Observe(now.Sub(msg.Timestamp).Seconds())
}
