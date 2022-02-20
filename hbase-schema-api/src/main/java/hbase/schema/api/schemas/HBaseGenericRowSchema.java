package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseCellParser;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.api.interfaces.HBaseWriteSchema;
import hbase.schema.api.interfaces.converters.*;
import hbase.schema.api.models.HBaseGenericRow;

import java.util.List;
import java.util.SortedMap;

import static hbase.schema.api.utils.HBaseSchemaUtils.sortedByteMap;
import static java.util.Collections.singletonList;

public class HBaseGenericRowSchema implements HBaseReadSchema<HBaseGenericRow>, HBaseWriteSchema<HBaseGenericRow> {
    @Override
    public HBaseBytesParser<HBaseGenericRow> getRowKeyParser() {
        return HBaseGenericRow::setRowKey;
    }

    @Override
    public HBaseBytesMapper<HBaseGenericRow> getRowKeyGenerator() {
        return HBaseGenericRow::getRowKey;
    }

    @Override
    public HBaseBytesMapper<HBaseGenericRow> getScanRowKeyGenerator() {
        return HBaseGenericRow::getRowKey;
    }

    @Override
    public List<HBaseCellParser<HBaseGenericRow>> getCellParsers() {
        return singletonList((genericRow, qualifier, value) ->
                initializeCells(genericRow).put(qualifier, value)
        );
    }

    @Override
    public HBaseLongMapper<HBaseGenericRow> getTimestampGenerator() {
        return HBaseGenericRow::getTimestampMs;
    }

    @Override
    public List<HBaseCellsMapper<HBaseGenericRow>> getPutGenerators() {
        return singletonList(HBaseGenericRow::getBytesCells);
    }

    @Override
    public List<HBaseLongsMapper<HBaseGenericRow>> getIncrementGenerators() {
        return singletonList(HBaseGenericRowSchema::initializeLongs);
    }

    private static SortedMap<byte[], byte[]> initializeCells(HBaseGenericRow genericRow) {
        SortedMap<byte[], byte[]> cellsMap = genericRow.getBytesCells();
        if (cellsMap == null) {
            cellsMap = sortedByteMap();
            genericRow.setBytesCells(cellsMap);
        }
        return cellsMap;
    }

    private static SortedMap<byte[], Long> initializeLongs(HBaseGenericRow genericRow) {
        SortedMap<byte[], Long> cellsMap = genericRow.getLongCells();
        if (cellsMap == null) {
            cellsMap = sortedByteMap();
            genericRow.setLongCells(cellsMap);
        }
        return cellsMap;
    }
}
