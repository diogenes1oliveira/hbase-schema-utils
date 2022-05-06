package hbase.schema.api.models;

import hbase.schema.api.interfaces.conversion.BytesConverter;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static hbase.schema.api.utils.ByteBufferComparator.BYTE_BUFFER_COMPARATOR;
import static hbase.schema.api.utils.BytePrefixComparator.BYTE_PREFIX_COMPARATOR;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hbase.util.Bytes.toBytesBinary;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

public class HBaseGenericRow {
    private final byte[] rowKey;
    private final List<ByteBuffer> rowKeyParts;
    private final NavigableMap<byte[], byte[]> cellsMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

    /**
     * @param rowKey    row key
     * @param cellsMap  cells binary data
     * @param separator binary separator to split the row key. If null, the row key is not split
     */
    public HBaseGenericRow(byte[] rowKey, NavigableMap<byte[], byte[]> cellsMap, byte[] separator) {
        this.rowKey = requireNonNull(rowKey, "row key can't be null");
        this.cellsMap.putAll(requireNonNull(cellsMap, "cells map can't be null"));
        this.rowKeyParts = unmodifiableList(splitBytes(rowKey, separator));
    }

    /**
     * @param rowKey   row key
     * @param cellsMap cells binary data
     */
    public HBaseGenericRow(byte[] rowKey, NavigableMap<byte[], byte[]> cellsMap) {
        this(rowKey, cellsMap, null);
    }

    /**
     * Copy constructor
     */
    public HBaseGenericRow(HBaseGenericRow other) {
        this.rowKey = other.rowKey;
        this.rowKeyParts = other.rowKeyParts;
        this.cellsMap.putAll(other.cellsMap);
    }

    /**
     * Copy constructor
     */
    public HBaseGenericRow(HBaseGenericRow other, byte[] separator) {
        this.rowKey = other.rowKey;
        this.cellsMap.putAll(other.cellsMap);
        this.rowKeyParts = unmodifiableList(splitBytes(other.rowKey, separator));
    }

    /**
     * Returns the row key bytes
     */
    public ByteBuffer getRowKey() {
        return ByteBuffer.wrap(rowKey).asReadOnlyBuffer();
    }

    /**
     * Returns the number of parts in the row key
     */
    public int getRowKeyPartsCount() {
        return rowKeyParts.size();
    }

    /**
     * Gets a row key binary part
     *
     * @param index index of the row key part
     * @return the row key part
     * @throws IndexOutOfBoundsException row key doesn't have that many parts
     */
    public ByteBuffer getRowKeyPart(int index) {
        return rowKeyParts.get(index).asReadOnlyBuffer();
    }

    /**
     * Gets a converted value of a row key binary part
     *
     * @param index index of the row key part
     * @param <T>   target converted type
     * @return the row key converted part
     * @throws IndexOutOfBoundsException row key doesn't have that many parts
     */
    public <T> T getRowKeyPart(int index, BytesConverter<T> converter) {
        ByteBuffer value = getRowKeyPart(index);
        return converter.fromBytes(value);
    }

    /**
     * Generates a printable map of the row cells map
     */
    public Map<String, String> toPrintableCellsMap() {
        Map<String, String> result = new LinkedHashMap<>();

        for (Map.Entry<byte[], byte[]> entry : cellsMap.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();
            result.put(toStringBinary(key), toStringBinary(value));
        }

        return result;
    }

    /**
     * Gets the binary value of a single column
     *
     * @param qualifier HBase column qualifier
     * @return column cell data or {@code null}
     */
    public ByteBuffer getColumn(byte[] qualifier) {
        byte[] value = cellsMap.get(qualifier);
        if (value == null) {
            return null;
        } else {
            return ByteBuffer.wrap(value).asReadOnlyBuffer();
        }
    }

    /**
     * Gets the converted value of a single column
     *
     * @param qualifier HBase column qualifier
     * @param converter typed bytes converter
     * @param <T>       target converted type
     * @return column cell converted data or {@code null}
     */
    public <T> T getColumn(byte[] qualifier, BytesConverter<T> converter) {
        ByteBuffer value = getColumn(qualifier);
        if (value == null) {
            return null;
        } else {
            return converter.fromBytes(value);
        }
    }

    /**
     * Removes the column from the cells map
     *
     * @param qualifier HBase column qualifier
     */
    public void removeColumn(byte[] qualifier) {
        cellsMap.remove(qualifier);
    }

    /**
     * Gets the map (qualifier -> value) cells with the given prefix
     *
     * @param prefix HBase column prefix
     */
    public NavigableMap<ByteBuffer, ByteBuffer> getPrefix(byte[] prefix) {
        NavigableMap<byte[], byte[]> tailMap = cellsMap.tailMap(prefix, true);

        NavigableMap<ByteBuffer, ByteBuffer> result = new TreeMap<>(BYTE_BUFFER_COMPARATOR);
        for (Map.Entry<byte[], byte[]> entry : tailMap.entrySet()) {
            byte[] qualifierBytes = entry.getKey();
            byte[] valueBytes = entry.getValue();
            if (valueBytes == null) {
                continue;
            } else if (BYTE_PREFIX_COMPARATOR.compare(qualifierBytes, prefix) > 0) {
                break;
            }

            ByteBuffer qualifier = (ByteBuffer) ByteBuffer.wrap(qualifierBytes).position(prefix.length);
            ByteBuffer value = ByteBuffer.wrap(valueBytes);

            result.put(qualifier.slice().asReadOnlyBuffer(), value.slice().asReadOnlyBuffer());
        }

        return result;
    }

    /**
     * Gets the converted map (qualifier -> value) cells with the given prefix
     *
     * @param prefix         HBase column prefix
     * @param keyConverter   typed converter for the column keys
     * @param valueConverter typed converter for the cell values
     * @return map of converted (keys -> values)
     */
    public <K, V> Map<K, V> getPrefix(byte[] prefix, BytesConverter<K> keyConverter, BytesConverter<V> valueConverter) {
        NavigableMap<ByteBuffer, ByteBuffer> prefixMap = getPrefix(prefix);
        Map<K, V> result = new LinkedHashMap<>();

        for (Map.Entry<ByteBuffer, ByteBuffer> entry : prefixMap.entrySet()) {
            K key = keyConverter.fromBytes(entry.getKey());
            V value = valueConverter.fromBytes(entry.getValue());

            if (key != null && value != null) {
                result.put(key, value);
            }
        }

        return result;
    }

    /**
     * Gets the converted map (qualifier -> value) cells with the given prefix
     *
     * @param prefix    HBase column prefix
     * @param converter typed converter for the column qualifier and values
     * @return map of converted (keys -> values)
     */
    public <T> Map<T, T> getPrefix(byte[] prefix, BytesConverter<T> converter) {
        return getPrefix(prefix, converter, converter);
    }

    /**
     * Converts the printable cells map into a cells map
     *
     * @param printableMap printable map such as generated by {@link #toPrintableCellsMap()}
     * @return binary cells map
     */
    public static NavigableMap<byte[], byte[]> fromPrintableCellsMap(Map<String, String> printableMap) {
        NavigableMap<byte[], byte[]> result = new TreeMap<>(Bytes.BYTES_COMPARATOR);

        for (Map.Entry<String, String> entry : printableMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null || value == null) {
                continue;
            }
            result.put(toBytesBinary(key), toBytesBinary(value));
        }

        return result;
    }

    public static List<ByteBuffer> splitBytes(byte[] bytes, byte[] separator) {
        if (separator == null) {
            return singletonList(ByteBuffer.wrap(bytes));
        }
        List<ByteBuffer> buffers = new ArrayList<>();

        for (int i = 0; i < bytes.length; ++i) {
            if (i + separator.length >= bytes.length) {
                buffers.add(ByteBuffer.wrap(bytes, i, bytes.length - i));
                break;
            }
            if (Bytes.compareTo(bytes, i, separator.length, separator, 0, separator.length) == 0) {
                buffers.add(ByteBuffer.wrap(bytes, i, separator.length));
                i += separator.length;
            }
        }

        return buffers;
    }

    @Override
    public String toString() {
        return "HBaseGenericRow{" +
                "rowKey=" + toStringBinary(rowKey) +
                ", cellsMap=" + toPrintableCellsMap() +
                '}';
    }

    /**
     * Considers just the {@link #rowKey} for equality
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HBaseGenericRow)) {
            return false;
        }
        HBaseGenericRow other = (HBaseGenericRow) o;
        return Arrays.equals(this.rowKey, other.rowKey);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(rowKey);
    }
}
