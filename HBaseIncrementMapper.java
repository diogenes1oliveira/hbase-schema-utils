package com.github.diogenes1oliveira.hbase.schema;

import org.apache.hadoop.hbase.client.Increment;

import java.util.Optional;

public interface HBaseIncrementMapper<T> extends HBaseMutationsMapper<T, Increment> {
    Optional<HBaseLongWriter<T>> deltaWriter(byte[] family, byte[] qualifier);

    default Optional<Long> toDelta(byte[] family, byte[] qualifier, T obj) {
        return deltaWriter(family, qualifier)
                .map(writer -> writer.toLong(obj));
    }

    @Override
    default Increment create(byte[] rowKey, long timestamp) {
        return new Increment(rowKey).setTimestamp(timestamp);
    }

    @Override
    default boolean addData(Increment increment, byte[] family, byte[] qualifier, T obj) {
        Long delta = toDelta(family, qualifier, obj).orElse(null);
        if (delta != null) {
            increment.addColumn(family, qualifier, delta);
            return true;
        } else {
            return false;
        }
    }
}
