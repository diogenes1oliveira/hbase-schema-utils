package com.github.diogenes1oliveira.hbase.schema;

import org.apache.hadoop.hbase.client.Put;

import java.util.List;
import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;

public interface HBasePutMapper<T> {
    Optional<HBaseBytesWriter<T>> valueWriter(byte[] family, byte[] qualifier);

    Optional<HBaseBytesWriter<T>> rowKeyWriter(byte[] family);

    Optional<HBaseLongWriter<T>> timestampWriter(byte[] family);

    List<byte[]> qualifiers(byte[] family, T obj);

    default Optional<byte[]> toValue(byte[] family, byte[] qualifier, T obj) {
        return valueWriter(family, qualifier)
                .map(writer -> writer.toBytes(obj));
    }

    default Optional<byte[]> toRowKey(byte[] family, T obj) {
        return rowKeyWriter(family)
                .map(writer -> writer.toBytes(obj));
    }

    default Optional<Long> toTimestamp(byte[] family, T obj) {
        return timestampWriter(family)
                .map(writer -> writer.toLong(obj));
    }

    default Optional<Put> toPut(byte[] family, T obj) {
        byte[] rowKey = toRowKey(family, obj).orElse(null);
        long timestamp = toTimestamp(family, obj).orElse(-1L);
        if (rowKey == null || timestamp == -1) {
            return empty();
        }
        Put put = new Put(rowKey).setTimestamp(timestamp);
        boolean hasData = false;

        for (byte[] qualifier : qualifiers(family, obj)) {
            byte[] value = toValue(family, qualifier, obj).orElse(null);
            if (value != null) {
                put.addColumn(family, qualifier, timestamp, value);
                hasData = true;
            }
        }

        if (hasData) {
            return of(put);
        } else {
            return empty();
        }
    }
}
