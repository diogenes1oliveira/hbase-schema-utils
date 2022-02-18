package com.github.diogenes1oliveira.hbase.schema;

import org.apache.hadoop.hbase.client.Mutation;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;

public interface HBaseMutationsMapper<T, M extends Mutation> {

    Optional<HBaseBytesWriter<T>> rowKeyWriter(byte[] family);

    Optional<HBaseLongWriter<T>> timestampWriter(byte[] family);

    List<byte[]> qualifiers(byte[] family, T obj);

    M create(byte[] rowKey, long timestamp);

    boolean addData(M mutation, byte[] family, byte[] qualifier, T obj);

    default Optional<byte[]> prefix(byte[] family) {
        return empty();
    }

    default Optional<byte[]> toRowKey(byte[] family, T obj) {
        return rowKeyWriter(family)
                .map(writer -> writer.toBytes(obj));
    }

    default Optional<Long> toTimestamp(byte[] family, T obj) {
        return timestampWriter(family)
                .map(writer -> writer.toLong(obj));
    }

    default List<M> toMutations(byte[] family, T obj) {
        byte[] rowKey = toRowKey(family, obj).orElse(null);
        long timestamp = toTimestamp(family, obj).orElse(-1L);
        if (rowKey == null || timestamp == -1) {
            return emptyList();
        }
        M mutation = create(rowKey, timestamp);
        boolean hasData = false;

        for (byte[] qualifier : qualifiers(family, obj)) {
            if (addData(mutation, family, qualifier, obj)) {
                hasData = true;
            }
        }

        if (hasData) {
            return singletonList(mutation);
        } else {
            return emptyList();
        }
    }
}
