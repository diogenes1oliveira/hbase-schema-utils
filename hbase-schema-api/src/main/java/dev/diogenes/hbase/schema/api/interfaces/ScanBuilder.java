package dev.diogenes.hbase.schema.api.interfaces;

import java.util.List;

import org.apache.hadoop.hbase.client.Scan;

import com.fasterxml.jackson.core.type.TypeReference;

public interface ScanBuilder<T> {
    TypeReference<T> queryType();

    List<Scan> build(T query);

    default void configure(String prop, String value) {
        // nothing to by default
    }
}
