package hbase.schema.api.interfaces;

import hbase.schema.api.models.HBaseValueCell;

import java.util.List;

public interface HBaseCellsParser<T> {
    void parseCells(T object, List<HBaseValueCell> cells);
}
