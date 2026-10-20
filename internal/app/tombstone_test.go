package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func rowModeMapping() TopicTableMapping {
	return TopicTableMapping{
		Topic:         "teams",
		Table:         "default.teams",
		KafkaMetadata: &KafkaMetadataMapping{Offset: "__offset"},
		Tombstones:    &TombstoneConfig{Mode: TombstoneModeRow, KeyColumn: "id", DeletedColumn: "_is_deleted"},
	}
}

func TestTombstoneRecord(t *testing.T) {
	cfg := TombstoneConfig{Mode: TombstoneModeRow, KeyColumn: "id", DeletedColumn: "_is_deleted"}

	rec, err := tombstoneRecord([]byte("42"), cfg, ColumnInfo{"id": "Int64", "_is_deleted": "UInt8"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec["id"] != int64(42) || rec["_is_deleted"] != uint8(1) || len(rec) != 2 {
		t.Fatalf("unexpected record: %#v", rec)
	}

	if _, err := tombstoneRecord(nil, cfg, ColumnInfo{"id": "Int64"}); err == nil || !strings.Contains(err.Error(), "without a key") {
		t.Fatalf("expected a missing-key error, got %v", err)
	}
}

func TestCoerceKey(t *testing.T) {
	id := uuid.MustParse("0190f4c2-6a1e-7b3c-9d4e-5f6a7b8c9d0e")
	cases := []struct {
		name    string
		key     string
		chType  string
		want    interface{}
		wantErr string
	}{
		{"Int64", " 42 ", "Int64", int64(42), ""},
		{"Nullable Int32", "-7", "Nullable(Int32)", int64(-7), ""},
		{"Int8 overflow", "300", "Int8", nil, "does not fit"},
		{"UInt64 above the int64 range", "18446744073709551615", "UInt64", uint64(18446744073709551615), ""},
		{"negative key on an unsigned column", "-1", "Nullable(UInt32)", nil, "does not fit"},
		{"LowCardinality UInt64", "9", "LowCardinality(Nullable(UInt64))", uint64(9), ""},
		{"non-numeric key on an integer column", "abc", "Int64", nil, "does not fit"},
		{"String", "team-1", "String", "team-1", ""},
		{"LowCardinality String", "team-1", "LowCardinality(String)", "team-1", ""},
		{"UUID", id.String(), "UUID", id, ""},
		{"invalid UUID", "018f-abc", "UUID", nil, "is not a UUID"},
		{"braced UUID is rejected like ClickHouse does", "{" + id.String() + "}", "Nullable(UUID)", nil, "is not a UUID"},
		{"FixedString that fits", "abcd", "FixedString(4)", "abcd", ""},
		{"FixedString too long", "abcde", "FixedString(4)", nil, "longer than"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := coerceKey([]byte(c.key), c.chType)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("expected error containing %q, got %v (value %#v)", c.wantErr, err, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %#v (%T), want %#v (%T)", got, got, c.want, c.want)
			}
		})
	}
}

func TestSupportedKeyType(t *testing.T) {
	for _, typ := range []string{"Int8", "UInt64", "Nullable(Int64)", "LowCardinality(String)", "String", "FixedString(16)", "UUID", "Nullable(UUID)"} {
		if !supportedKeyType(typ) {
			t.Errorf("expected %s to be a supported key type", typ)
		}
	}
	for _, typ := range []string{"Int128", "UInt256", "Float64", "Date", "DateTime64(3)", "Decimal(10, 2)", "IntervalDay", "Array(String)"} {
		if supportedKeyType(typ) {
			t.Errorf("expected %s to be rejected as a key type", typ)
		}
	}
}

func TestIsRowTombstone(t *testing.T) {
	row := rowModeMapping()
	skip := rowModeMapping()
	skip.Tombstones = nil

	if !isRowTombstone(nil, &row) {
		t.Error("a null value in row mode is a tombstone")
	}
	if isRowTombstone([]byte{}, &row) {
		t.Error("an empty but non-null value is data, not a tombstone")
	}
	if isRowTombstone([]byte(`{"id":1}`), &row) {
		t.Error("a value is not a tombstone")
	}
	if isRowTombstone(nil, &skip) {
		t.Error("without row mode a null value is never written as a deletion row")
	}
}

func TestValidateTombstoneColumns(t *testing.T) {
	full := ColumnInfo{"id": "Int64", "name": "String", "_is_deleted": "UInt8", "__offset": "UInt64"}

	if err := validateTombstoneColumns(rowModeMapping(), full); err != nil {
		t.Fatalf("unexpected error for a complete table: %v", err)
	}

	skip := rowModeMapping()
	skip.Tombstones.Mode = TombstoneModeSkip
	if err := validateTombstoneColumns(skip, ColumnInfo{}); err != nil {
		t.Fatalf("skip mode must not validate columns, got %v", err)
	}

	cases := []struct {
		name    string
		drop    string
		retype  map[string]string
		wantErr string
	}{
		{"missing key column", "id", nil, "no tombstones.key_column"},
		{"missing deleted column", "_is_deleted", nil, "no tombstones.deleted_column"},
		{"deleted column of the wrong type", "", map[string]string{"_is_deleted": "Bool"}, "must be UInt8"},
		{"missing offset column", "__offset", nil, "no kafka_metadata.offset column"},
		{"key column of an unsupported type", "", map[string]string{"id": "Float64"}, "supported: String"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cols := ColumnInfo{}
			for k, v := range full {
				if k != c.drop {
					cols[k] = v
				}
			}
			for k, v := range c.retype {
				cols[k] = v
			}
			err := validateTombstoneColumns(rowModeMapping(), cols)
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("expected error containing %q, got %v", c.wantErr, err)
			}
		})
	}
}

func TestSplitTombstoneRows(t *testing.T) {
	batch := []map[string]interface{}{
		{"id": int64(1), "name": "a"},
		{"id": int64(2), "_is_deleted": uint8(1)},
		{"id": int64(3), "name": "c", "_is_deleted": uint8(0)},
		{"id": int64(4), "_is_deleted": uint8(1)},
		// A payload field named like the deleted column decodes as float64
		// (JSON) or int32 (Avro), never uint8: it stays a data row.
		{"id": int64(5), "_is_deleted": float64(1)},
		{"id": int64(6), "_is_deleted": int32(1)},
	}
	data, tombstones := splitTombstoneRows(batch, "_is_deleted")
	if len(data) != 4 || data[0]["id"] != int64(1) || data[1]["id"] != int64(3) || data[2]["id"] != int64(5) || data[3]["id"] != int64(6) {
		t.Fatalf("unexpected data rows: %#v", data)
	}
	if len(tombstones) != 2 || tombstones[0]["id"] != int64(2) || tombstones[1]["id"] != int64(4) {
		t.Fatalf("unexpected tombstone rows: %#v", tombstones)
	}
}

// recordingConn records the INSERTs writeBatch prepares and fails the Send of
// the INSERT numbered failSend (1-based; 0 never fails).
type recordingConn struct {
	driver.Conn
	inserts  []string
	rows     [][][]interface{}
	failSend int
}

type recordingBatch struct {
	driver.Batch
	conn *recordingConn
	n    int
}

func (c *recordingConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.inserts = append(c.inserts, query)
	c.rows = append(c.rows, nil)
	return &recordingBatch{conn: c, n: len(c.inserts)}, nil
}

func (b *recordingBatch) Append(v ...interface{}) error {
	b.conn.rows[b.n-1] = append(b.conn.rows[b.n-1], v)
	return nil
}

func (b *recordingBatch) Send() error {
	if b.n == b.conn.failSend {
		return errors.New("code: 53, message: type mismatch")
	}
	return nil
}

func TestWriteBatchParts(t *testing.T) {
	cols := ColumnInfo{"id": "Int64", "name": "String", "_is_deleted": "UInt8", "__offset": "UInt64"}
	batch := []map[string]interface{}{
		{"id": int64(1), "name": "a", "__offset": int64(10)},
		{"id": int64(1), "_is_deleted": uint8(1), "__offset": int64(11)},
		{"id": int64(2), "name": "b", "__offset": int64(12)},
	}
	newTask := func(conn driver.Conn, mapping TopicTableMapping) *SinkTask {
		mapping.MaxRetries = intPtr(0)
		mapping.RetryBackoffMs = intPtr(0)
		return &SinkTask{mapping: mapping, chConn: conn, columnInfo: cols, sugar: zap.NewNop().Sugar()}
	}

	t.Run("data and deletion rows go in separate INSERTs over their own columns", func(t *testing.T) {
		conn := &recordingConn{}
		if _, _, err := newTask(conn, rowModeMapping()).writeBatchParts(context.Background(), "default.teams", batch); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(conn.inserts) != 2 {
			t.Fatalf("expected 2 INSERTs, got %d: %q", len(conn.inserts), conn.inserts)
		}
		if !strings.Contains(conn.inserts[0], "(`__offset`, `id`, `name`)") || len(conn.rows[0]) != 2 {
			t.Errorf("unexpected data INSERT %q with %d rows", conn.inserts[0], len(conn.rows[0]))
		}
		if !strings.Contains(conn.inserts[1], "(`__offset`, `_is_deleted`, `id`)") || len(conn.rows[1]) != 1 {
			t.Errorf("unexpected deletion INSERT %q with %d rows", conn.inserts[1], len(conn.rows[1]))
		}
	})

	t.Run("a failed deletion INSERT fails the whole write", func(t *testing.T) {
		conn := &recordingConn{failSend: 2}
		if _, _, err := newTask(conn, rowModeMapping()).writeBatchParts(context.Background(), "default.teams", batch); err == nil {
			t.Fatal("expected the second INSERT's failure to be returned so no offset is committed")
		}
	})

	t.Run("a failed data INSERT stops before the deletion rows", func(t *testing.T) {
		conn := &recordingConn{failSend: 1}
		if _, _, err := newTask(conn, rowModeMapping()).writeBatchParts(context.Background(), "default.teams", batch); err == nil {
			t.Fatal("expected an error")
		}
		if len(conn.inserts) != 1 {
			t.Fatalf("expected the deletion INSERT not to run, got %d INSERTs", len(conn.inserts))
		}
	})

	t.Run("a batch of only deletion rows is one INSERT", func(t *testing.T) {
		conn := &recordingConn{}
		if _, _, err := newTask(conn, rowModeMapping()).writeBatchParts(context.Background(), "default.teams", batch[1:2]); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(conn.inserts) != 1 {
			t.Fatalf("expected 1 INSERT, got %d", len(conn.inserts))
		}
	})

	t.Run("skip mode writes the batch as one INSERT", func(t *testing.T) {
		conn := &recordingConn{}
		skip := rowModeMapping()
		skip.Tombstones = &TombstoneConfig{Mode: TombstoneModeSkip, DeletedColumn: DefaultTombstoneDeletedColumn}
		if _, _, err := newTask(conn, skip).writeBatchParts(context.Background(), "default.teams", batch); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(conn.inserts) != 1 {
			t.Fatalf("expected 1 INSERT, got %d", len(conn.inserts))
		}
	})
}
