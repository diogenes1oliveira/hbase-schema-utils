package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseResultParserSchema;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;

/**
 * Base class to aid in the implementation of a {@link HBaseResultParserSchema} based on single cells and cell prefixes
 *
 * @param <T> result object type
 */
public abstract class AbstractHBaseResultParserSchema<T> implements HBaseResultParserSchema<T> {
    /**
     * Populates the object with data from the row key
     * <p>
     * The default implementation just returns {@code false}
     *
     * @param obj    result object instance
     * @param rowKey row key bytes
     * @return true if some data was set
     */
    public boolean setFromRowKey(T obj, byte[] rowKey) {
        return false;
    }

    /**
     * Set of qualifier prefixes
     * <p>
     * Each set of cells with the given prefix will be parsed together.
     * <p>
     * The default implementation returns an empty set
     */
    public NavigableSet<byte[]> getPrefixes() {
        return asBytesTreeSet();
    }

    /**
     * Populates the object with the value of a single cell
     * <p>
     * The default implementation just returns {@code false}
     *
     * @param obj       object to be populated
     * @param qualifier HBase cell qualifier
     * @param value     HBase cell value
     * @return true if some data was set
     */
    public boolean setFromCell(T obj, byte[] qualifier, byte[] value) {
        return false;
    }

    /**
     * Populates the object with the values of multiple cells with the same prefix
     * <p>
     * The default implementation just returns {@code false}
     *
     * @param obj             object to be populated
     * @param prefix          qualifier prefix
     * @param cellsFromPrefix map of (qualifier -> cell value). The prefix is removed from the qualifier
     * @return true if some data was set
     */
    public boolean setFromPrefix(T obj, byte[] prefix, NavigableMap<byte[], byte[]> cellsFromPrefix) {
        return false;
    }

    /**
     * Populates the object with the fetched cells
     *
     * @param obj         result object instance
     * @param resultCells map of (qualifier -> cell value) fetched from HBase
     * @return true if some data was set
     */
    @Override
    public boolean setFromResult(T obj, byte[] rowKey, NavigableMap<byte[], byte[]> resultCells) {
        boolean setData = setFromRowKey(obj, rowKey);

        NavigableSet<byte[]> prefixes = getPrefixes();
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> prefixMap = asBytesTreeMap();

        for (Map.Entry<byte[], byte[]> entry : resultCells.entrySet()) {
            byte[] qualifier = entry.getKey();
            byte[] value = entry.getValue();
            byte[] prefix = prefixes.floor(qualifier);
            byte[] unprefixedQualifier = removePrefix(qualifier, prefix);
            if (unprefixedQualifier == null) {
                setData = setFromCell(obj, qualifier, value) || setData;
            } else {
                NavigableMap<byte[], byte[]> cellsFromPrefix = prefixMap.computeIfAbsent(prefix, b -> asBytesTreeMap());
                cellsFromPrefix.put(unprefixedQualifier, value);
            }
        }

        for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> entry : prefixMap.entrySet()) {
            byte[] prefix = entry.getKey();
            NavigableMap<byte[], byte[]> cellsFromPrefix = entry.getValue();
            setData = setFromPrefix(obj, prefix, cellsFromPrefix) || setData;
        }

        return setData;
    }

    private static byte @Nullable [] removePrefix(byte[] arr, byte @Nullable [] prefix) {
        if (prefix == null) {
            return null;
        }
        if (arr.length < prefix.length) {
            return null;
        }

        for (int i = 0; i < prefix.length; ++i) {
            if (arr[i] != prefix[i]) {
                return null;
            }
        }

        return Arrays.copyOfRange(arr, prefix.length, arr.length);
    }
}
