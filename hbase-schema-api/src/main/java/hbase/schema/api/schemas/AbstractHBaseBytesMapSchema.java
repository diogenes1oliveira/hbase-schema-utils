package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseCellParser;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.api.interfaces.HBaseWriteSchema;
import hbase.schema.api.interfaces.converters.*;

import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;

import static hbase.schema.api.utils.HBaseSchemaUtils.findCommonPrefix;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static java.util.Collections.*;

public abstract class AbstractHBaseBytesMapSchema implements HBaseReadSchema<SortedMap<byte[], byte[]>>, HBaseWriteSchema<SortedMap<byte[], byte[]>> {
    @Override
    public abstract HBaseBytesMapper<SortedMap<byte[], byte[]>> getRowKeyGenerator();

    @Override
    public abstract HBaseLongMapper<SortedMap<byte[], byte[]>> getTimestampGenerator();

    public abstract SortedSet<byte[]> getMappedFields();

    @Override
    public SortedMap<byte[], byte[]> newInstance() {
        return emptySortedMap();
    }

    @Override
    public HBaseBytesParser<SortedMap<byte[], byte[]>> getRowKeyParser() {
        // DEFAULT
        return HBaseBytesParser.dummy();
    }

    @Override
    public HBaseBytesMapper<SortedMap<byte[], byte[]>> getScanRowKeyGenerator() {
        // DEFAULT
        return getRowKeyGenerator();
    }

    @Override
    public List<HBaseCellParser<SortedMap<byte[], byte[]>>> getCellParsers() {
        SortedSet<byte[]> fields = getMappedFields();

        return singletonList((map, qualifier, value) -> {
            if (fields.contains(qualifier)) {
                map.put(qualifier, value);
            }
        });
    }

    @Override
    public SortedSet<byte[]> getQualifiers(SortedMap<byte[], byte[]> query) {
        return getMappedFields();
    }

    @Override
    public SortedSet<byte[]> getQualifierPrefixes(SortedMap<byte[], byte[]> query) {
        byte[] commonPrefix = findCommonPrefix(getQualifiers(query));
        if (commonPrefix != null) {
            return asBytesTreeSet(commonPrefix);
        } else {
            return null;
        }
    }

    @Override
    public List<HBaseCellsMapper<SortedMap<byte[], byte[]>>> getPutGenerators() {
        return singletonList(map -> map);
    }

    @Override
    public List<HBaseLongsMapper<SortedMap<byte[], byte[]>>> getIncrementGenerators() {
        return emptyList();
    }

}
