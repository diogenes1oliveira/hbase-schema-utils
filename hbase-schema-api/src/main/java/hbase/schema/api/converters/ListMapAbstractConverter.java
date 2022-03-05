package hbase.schema.api.converters;

import hbase.schema.api.interfaces.conversion.BytesMapConverter;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

/**
 * Base class to map lists into key-values of {@code byte[]} maps
 *
 * @param <T> item type
 */
public abstract class ListMapAbstractConverter<T> implements BytesMapConverter<List<T>> {
    /**
     * Extracts a {@code byte[]} key from the item
     */
    public abstract byte @Nullable [] toBytesKey(int index, T item);

    /**
     * Extracts a {@code byte[]} value from the item
     */
    public abstract byte @Nullable [] toBytesValue(int index, T item);

    /**
     * Parses a key-value pair into an item
     *
     * @param key   binary key
     * @param value binary value
     */
    public abstract @Nullable T fromBytes(byte[] key, byte[] value);

    /**
     * Converts the list to a bytes map by applying {@link #toBytesKey(int, T)} and {@link #toBytesValue(int, T)}
     * to each item
     */
    @Override
    public NavigableMap<byte[], byte[]> toBytesMap(List<T> list) {
        NavigableMap<byte[], byte[]> bytesMap = asBytesTreeMap();

        for (int i = 0; i < list.size(); ++i) {
            T item = list.get(i);
            byte[] key = toBytesKey(i, item);
            byte[] value = toBytesValue(i, item);
            if (key != null && value != null) {
                bytesMap.put(key, value);
            }
        }

        return bytesMap;
    }

    /**
     * Converts the bytes map to a list by applying {@link #fromBytes(byte[], byte[])} for each key-value pair
     */
    @Override
    public List<T> fromBytesMap(NavigableMap<byte[], byte[]> bytesMap) {
        List<T> list = new ArrayList<>();

        for (Map.Entry<byte[], byte[]> entry : bytesMap.entrySet()) {
            T item = fromBytes(entry.getKey(), entry.getValue());
            if (item != null) {
                list.add(item);
            }
        }

        return list;
    }

    /**
     * {@code List.class}
     */
    @Override
    public Class<?> type() {
        return List.class;
    }
}
