package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseCellParser;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.api.interfaces.HBaseWriteSchema;
import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import hbase.schema.api.interfaces.converters.HBaseBytesParser;
import hbase.schema.api.interfaces.converters.HBaseCellsMapper;
import hbase.schema.api.interfaces.converters.HBaseLongMapper;
import hbase.schema.api.interfaces.converters.HBaseLongsMapper;
import hbase.schema.api.models.HBaseGenericRow;

import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.SortedSet;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
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
    public SortedSet<byte[]> getQualifiers(HBaseGenericRow query) {
        return query.getBytesCells().navigableKeySet();
    }

    @Override
    public SortedSet<byte[]> getQualifierPrefixes(HBaseGenericRow query) {
        return asBytesTreeSet(new byte[0]);
    }

    @Override
    public List<HBaseCellParser<HBaseGenericRow>> getCellParsers() {
        return singletonList((genericRow, qualifier, value) ->
                initializeCells(genericRow).put(qualifier, value)
        );
    }

    @Override
    public HBaseGenericRow newInstance() {
        return new HBaseGenericRow(new byte[0], asBytesTreeMap());
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
        NavigableMap<byte[], byte[]> cellsMap = genericRow.getBytesCells();
        if (cellsMap == null) {
            cellsMap = asBytesTreeMap();
            genericRow.setBytesCells(cellsMap);
        }
        return cellsMap;
    }

    private static SortedMap<byte[], Long> initializeLongs(HBaseGenericRow genericRow) {
        NavigableMap<byte[], Long> cellsMap = genericRow.getLongCells();
        if (cellsMap == null) {
            cellsMap = asBytesTreeMap();
            genericRow.setLongCells(cellsMap);
        }
        return cellsMap;
    }
}
