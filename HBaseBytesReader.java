package com.github.diogenes1oliveira.hbase.schema;

@FunctionalInterface
public interface HBaseBytesReader<T> {
    void parseBytes(byte[] value, T obj);
}
