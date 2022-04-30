package hbase.schema.api.schema;

import hbase.schema.api.interfaces.HBaseResultParser;
import hbase.schema.api.models.HBaseValueCell;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static java.util.Collections.unmodifiableSet;

public abstract class AbstractHBaseResultParser<T> implements HBaseResultParser<T> {
    private static final Set<byte[]> EMPTY = unmodifiableSet(asBytesTreeSet());

    public Set<byte[]> prefixes() {
        return EMPTY;
    }

    public void parseCell(T obj, HBaseValueCell cell) {
        // nothing to do, by default
    }

    public void parseCells(T obj, byte[] prefix, NavigableMap<byte[], byte[]> prefixMap) {
        // nothing to do, by default
    }

    @Override
    public final void parseCells(T obj, List<HBaseValueCell> cells) {
        List<HBaseValueCell> cellsCopy = new ArrayList<>(cells);

        for (byte[] prefix : prefixes()) {
            NavigableMap<byte[], byte[]> prefixMap = HBaseValueCell.withoutPrefix(prefix, cellsCopy);
            parseCells(obj, prefix, prefixMap);
        }

        for (HBaseValueCell cell : cells) {
            parseCell(obj, cell);
        }
    }
}
