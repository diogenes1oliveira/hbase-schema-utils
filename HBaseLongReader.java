package com.github.diogenes1oliveira.hbase.schema;

import static com.github.diogenes1oliveira.hbase.utils.PayloadUtils.bytesToLong;

@FunctionalInterface
public interface HBaseLongReader<T> extends HBaseBytesReader<T> {
    void parseLong(long value, T obj);

    @Override
    default void parseBytes(byte[] bytes, T obj) {
        long value = bytesToLong(bytes);
        parseLong(value, obj);
    }
}
