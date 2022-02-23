package hbase.schema.api.schemas;

import edu.umd.cs.findbugs.annotations.Nullable;
import hbase.schema.api.interfaces.HBaseResultParser;
import hbase.schema.api.interfaces.converters.TriConsumer;
import hbase.schema.api.utils.HBaseSchemaUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

public class HBaseFunctionalResultParser<T> implements HBaseResultParser<T> {
    private final Supplier<T> constructor;
    private final BiConsumer<T, byte[]> fromRowKeySetter;
    private final List<TriConsumer<T, byte[], byte[]>> cellSetters;

    public HBaseFunctionalResultParser(Supplier<T> constructor,
                                       BiConsumer<T, byte[]> fromRowKeySetter,
                                       List<TriConsumer<T, byte[], byte[]>> cellSetters) {
        this.constructor = constructor;
        this.fromRowKeySetter = fromRowKeySetter;
        this.cellSetters = cellSetters;
    }

    @Override
    public T newInstance() {
        return constructor.get();
    }

    @Override
    public void setFromRowKey(T obj, byte[] rowKey) {
        fromRowKeySetter.accept(obj, rowKey);
    }

    @SuppressWarnings("SimplifyOptionalCallChains")
    @Override
    public void setFromResult(T obj, NavigableMap<byte[], byte[]> cellsMap) {
        for (Map.Entry<byte[], byte[]> cellEntry : cellsMap.entrySet()) {
            for(TriConsumer<T, byte[], byte[]> cellSetter: cellSetters) {

            }
        }
    }

    @Override
    public void setFromCell(T obj, byte[] qualifier, byte[] value) {
        setFromCell(obj, fromCellSetters.get(qualifier), value);
    }

    @Override
    public void setFromPrefix(T obj, byte[] prefix, NavigableMap<byte[], byte[]> cellsMap) {
        BiConsumer<T, NavigableMap<byte[], byte[]>> prefixSetter =
                ofNullable(fromPrefixCellSetters.subMap(prefix, true, prefix, true).firstEntry())
                        .filter(e -> Bytes.equals(e.getKey(), prefix))
                        .map(Map.Entry::getValue)
                        .orElse(null);

        setFromPrefix(obj, prefix, prefixSetter, cellsMap);
    }

    private boolean setFromCell(T obj, @Nullable BiConsumer<T, byte[]> setter, byte[] value) {
        if (setter != null) {
            setter.accept(obj, value);
            return true;
        } else {
            return false;
        }
    }

    private void setFromPrefix(T obj,
                               byte[] prefix,
                               @Nullable BiConsumer<T, NavigableMap<byte[], byte[]>> prefixSetter,
                               NavigableMap<byte[], byte[]> cellsMap) {
        if (prefixSetter != null) {
            TreeMap<byte[], byte[]> subMap = sliceKeys(cellsMap, prefix);
            prefixSetter.accept(obj, subMap);
        }
    }

    private static TreeMap<byte[], byte[]> sliceKeys(NavigableMap<byte[], byte[]> cellsMap, byte[] prefix) {
        // TODO: try not to do this copy, but the methods in NavigableMap don't seem to work for this
        return cellsMap.entrySet()
                       .stream()
                       .filter(e -> matchesPrefix(e.getKey(), prefix))
                       .collect(toMap(
                               e -> Arrays.copyOfRange(e.getKey(), prefix.length, e.getKey().length),
                               Map.Entry::getValue,
                               (a, b) -> b,
                               HBaseSchemaUtils::asBytesTreeMap
                       ));
    }

    private static boolean matchesPrefix(byte[] value, byte[] prefix) {
        for (int i = 0; i < prefix.length; ++i) {
            if (i >= value.length) {
                return false;
            }
            if (value[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
