package hbase.schema.api.interfaces;

import hbase.schema.api.models.HBaseValueCell;

import java.util.List;

public interface HBaseResultParser<T> {
    void parseCells(T obj, List<HBaseValueCell> cells);

    default void parseRowKey(T obj, byte[] rowKey) {
        // nothing to do
    }

    T newInstance();
}
