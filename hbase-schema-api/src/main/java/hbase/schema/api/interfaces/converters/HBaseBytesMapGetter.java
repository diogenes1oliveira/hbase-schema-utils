package hbase.schema.api.interfaces.converters;

import java.util.Map;
import java.util.NavigableMap;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

/**
 * Interface to extract a map ({@code byte[]} -> {@code byte[]}) from a Java object
 *
 * @param <T> object type
 */
@FunctionalInterface
public interface HBaseBytesMapGetter<T> {
    NavigableMap<byte[], byte[]> getBytesMap(T obj);

    /**
     * Builds a map getter that always returns an empty map
     *
     * @param <T> object type
     * @return map getter instance
     */
    static <T> HBaseBytesMapGetter<T> empty() {
        NavigableMap<byte[], byte[]> result = asBytesTreeMap();
        return obj -> result;
    }

    /**
     * Builds a new {@link HBaseBytesMapGetter} object from a bytes-conversible map field
     *
     * @param getter             lambda to get the field map from the object
     * @param qualifierConverter lambda to convert the key into a {@code byte[]} qualifier
     * @param valueConverter     lambda to convert the key into a {@code byte[]} value
     * @param <T>                object type
     * @param <K>                key type
     * @param <V>                value type
     * @return bytes map getter instance
     */
    static <T, K, V> HBaseBytesMapGetter<T> bytesMapGetter(Function<T, ? extends Map<K, V>> getter,
                                                           HBaseBytesGetter<K> qualifierConverter,
                                                           HBaseBytesGetter<V> valueConverter) {
        return obj -> {
            NavigableMap<byte[], byte[]> result = asBytesTreeMap();

            Map<K, V> mapField = getter.apply(obj);
            if (mapField == null) {
                return result;
            }

            for (Map.Entry<K, V> entry : mapField.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                if (key == null || value == null) {
                    continue;
                }
                result.put(qualifierConverter.getBytes(key), valueConverter.getBytes(value));
            }

            return result;
        };
    }
}
