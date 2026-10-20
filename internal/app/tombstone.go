package app

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// isRowTombstone reports whether a message is a tombstone to write as a deletion
// row. Only a null value counts: an empty but non-null payload is data, and
// still goes through the decoder.
func isRowTombstone(value []byte, mapping *TopicTableMapping) bool {
	return value == nil && mapping.writesTombstoneRows()
}

// tombstoneRecord builds the row a tombstone is written as in row mode: the key,
// bound to the key column's type, and the deleted flag. Every other column is
// left out so ClickHouse fills its default; flush writes these rows in their own
// INSERT for exactly that reason (see splitTombstoneRows).
func tombstoneRecord(key []byte, cfg TombstoneConfig, columnInfo ColumnInfo) (map[string]interface{}, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("tombstone without a key: nothing to delete")
	}
	value, err := coerceKey(key, columnInfo[cfg.KeyColumn])
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		cfg.KeyColumn:     value,
		cfg.DeletedColumn: uint8(1),
	}, nil
}

// keyBaseType strips the Nullable and LowCardinality wrappers from a ClickHouse
// type, which do not change how a key is bound.
func keyBaseType(chType string) string {
	t := strings.TrimSpace(chType)
	for {
		switch {
		case strings.HasPrefix(t, "Nullable(") && strings.HasSuffix(t, ")"):
			t = t[len("Nullable(") : len(t)-1]
		case strings.HasPrefix(t, "LowCardinality(") && strings.HasSuffix(t, ")"):
			t = t[len("LowCardinality(") : len(t)-1]
		default:
			return t
		}
	}
}

// keyIntBits returns the bit size and signedness of an integer key type up to
// 64 bits, or ok=false for any other type.
func keyIntBits(base string) (bits int, unsigned bool, ok bool) {
	unsigned = strings.HasPrefix(base, "UInt")
	digits := strings.TrimPrefix(strings.TrimPrefix(base, "U"), "Int")
	if !strings.HasPrefix(base, "Int") && !unsigned {
		return 0, false, false
	}
	switch digits {
	case "8", "16", "32", "64":
		bits, _ = strconv.Atoi(digits)
		return bits, unsigned, true
	}
	return 0, false, false
}

// fixedStringSize returns N for a FixedString(N) type.
func fixedStringSize(base string) (int, bool) {
	if !strings.HasPrefix(base, "FixedString(") || !strings.HasSuffix(base, ")") {
		return 0, false
	}
	n, err := strconv.Atoi(base[len("FixedString(") : len(base)-1])
	return n, err == nil
}

// supportedKeyType reports whether coerceKey can bind a message key to a column
// of this type, so a key that does not fit is a decode error and not a write
// error that would fail the whole batch.
func supportedKeyType(chType string) bool {
	base := keyBaseType(chType)
	if _, _, ok := keyIntBits(base); ok {
		return true
	}
	if _, ok := fixedStringSize(base); ok {
		return true
	}
	return base == "String" || base == "UUID"
}

// coerceKey converts the raw Kafka message key, read as text, into a value
// clickhouse-go binds against the key column: a sized integer for integer
// columns (unsigned ones reject negative keys instead of wrapping), a parsed
// UUID for UUID columns, a string otherwise. Every key the column would reject
// is rejected here.
func coerceKey(key []byte, chType string) (interface{}, error) {
	s := string(key)
	base := keyBaseType(chType)
	if bits, unsigned, ok := keyIntBits(base); ok {
		trimmed := strings.TrimSpace(s)
		if unsigned {
			n, err := strconv.ParseUint(trimmed, 10, bits)
			if err != nil {
				return nil, fmt.Errorf("tombstone key %q does not fit column type %s: %w", s, chType, err)
			}
			return n, nil
		}
		n, err := strconv.ParseInt(trimmed, 10, bits)
		if err != nil {
			return nil, fmt.Errorf("tombstone key %q does not fit column type %s: %w", s, chType, err)
		}
		return n, nil
	}
	if base == "UUID" {
		id, err := uuid.Parse(s)
		if err != nil || len(s) != 36 {
			return nil, fmt.Errorf("tombstone key %q is not a UUID but column type is %s", s, chType)
		}
		return id, nil
	}
	if n, ok := fixedStringSize(base); ok && len(key) > n {
		return nil, fmt.Errorf("tombstone key %q is longer than column type %s", s, chType)
	}
	return s, nil
}

// validateTombstoneColumns checks that the target table has the columns a
// deletion row writes, with types it can bind. A missing column would otherwise
// be dropped silently by writeBatch, and the "deletion" would land as an
// ordinary row full of defaults. Callers treat a failure like a table that did
// not validate: the topic is parked, not started.
func validateTombstoneColumns(mapping TopicTableMapping, columnInfo ColumnInfo) error {
	if !mapping.writesTombstoneRows() || columnInfo == nil {
		return nil
	}
	cfg := mapping.Tombstones
	keyType, ok := columnInfo[cfg.KeyColumn]
	if !ok {
		return fmt.Errorf("table %s has no tombstones.key_column %q", mapping.Table, cfg.KeyColumn)
	}
	if !supportedKeyType(keyType) {
		return fmt.Errorf("tombstones.key_column %q has type %s; supported: String, FixedString, UUID, (U)Int8 to (U)Int64, optionally Nullable or LowCardinality", cfg.KeyColumn, keyType)
	}
	deletedType, ok := columnInfo[cfg.DeletedColumn]
	if !ok {
		return fmt.Errorf("table %s has no tombstones.deleted_column %q", mapping.Table, cfg.DeletedColumn)
	}
	if deletedType != "UInt8" {
		return fmt.Errorf("tombstones.deleted_column %q must be UInt8 (ReplacingMergeTree is_deleted), got %s", cfg.DeletedColumn, deletedType)
	}
	if offsetCol := mapping.KafkaMetadata.Offset; offsetCol != "" {
		if _, ok := columnInfo[offsetCol]; !ok {
			return fmt.Errorf("table %s has no kafka_metadata.offset column %q", mapping.Table, offsetCol)
		}
	}
	return nil
}

// isDeletionRow reports whether record is a deletion row built by
// tombstoneRecord. Decoders never produce a uint8, so a payload field with the
// same name as the deleted column cannot be mistaken for one.
func isDeletionRow(record map[string]interface{}, deletedColumn string) bool {
	v, ok := record[deletedColumn]
	return ok && v == uint8(1)
}

// splitTombstoneRows separates deletion rows from data rows so each goes in its
// own INSERT. One INSERT over the union of columns would bind NULL into the data
// columns of a deletion row (and into the deleted column of a data row), which
// non-Nullable columns reject. Order between the two INSERTs does not matter:
// the ReplacingMergeTree version, the Kafka offset, decides which row wins.
func splitTombstoneRows(batch []map[string]interface{}, deletedColumn string) (data, tombstones []map[string]interface{}) {
	for _, record := range batch {
		if isDeletionRow(record, deletedColumn) {
			tombstones = append(tombstones, record)
			continue
		}
		data = append(data, record)
	}
	return data, tombstones
}
