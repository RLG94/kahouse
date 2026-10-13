package app

import (
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserveReceiveLag(t *testing.T) {
	recordReceiveLag.Reset()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)

	observeReceiveLag("orders", &kafka.Message{Timestamp: now.Add(-3 * time.Second), TimestampType: kafka.TimestampCreateTime}, now)
	observeReceiveLag("orders", &kafka.Message{Timestamp: now.Add(-1500 * time.Millisecond), TimestampType: kafka.TimestampLogAppendTime}, now)
	observeReceiveLag("orders", &kafka.Message{TimestampType: kafka.TimestampNotAvailable}, now)

	expected := `
# HELP kahouse_record_receive_lag_seconds Time from a message's Kafka timestamp to its read by the sink task
# TYPE kahouse_record_receive_lag_seconds summary
kahouse_record_receive_lag_seconds_sum{topic="orders"} 4.5
kahouse_record_receive_lag_seconds_count{topic="orders"} 2
`
	if err := testutil.CollectAndCompare(recordReceiveLag, strings.NewReader(expected), "kahouse_record_receive_lag_seconds"); err != nil {
		t.Fatal(err)
	}
}
