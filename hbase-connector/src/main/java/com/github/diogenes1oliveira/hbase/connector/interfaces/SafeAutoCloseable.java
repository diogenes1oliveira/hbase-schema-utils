package com.github.diogenes1oliveira.hbase.connector.interfaces;

public interface SafeAutoCloseable extends AutoCloseable {
    @Override
    default void close() {

    }
}
