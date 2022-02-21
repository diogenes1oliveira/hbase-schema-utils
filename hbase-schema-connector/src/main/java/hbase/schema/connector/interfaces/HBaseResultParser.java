package hbase.schema.connector.interfaces;

import hbase.schema.api.interfaces.HBaseReadSchema;

import java.util.SortedMap;

public interface HBaseResultParser {
    <T> void parse(T obj, byte[] rowKey, SortedMap<byte[], byte[]> cells, HBaseReadSchema<T> readSchema);
}
