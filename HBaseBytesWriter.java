package com.github.diogenes1oliveira.hbase.schema;

@FunctionalInterface
public interface HBaseBytesWriter<T> {
    byte[] toBytes(T obj);
}
