package com.github.diogenes1oliveira.hbase.schema;

import org.apache.hadoop.hbase.util.Bytes;

@FunctionalInterface
public interface HBaseLongWriter<T> extends HBaseBytesWriter<T> {
    Long toLong(T obj);

    @Override
    default byte[] toBytes(T obj) {
        Long l = toLong(obj);
        if (l != null) {
            return Bytes.toBytes(l);
        } else {
            return null;
        }
    }
}
