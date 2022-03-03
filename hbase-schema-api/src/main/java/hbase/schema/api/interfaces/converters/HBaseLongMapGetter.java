package hbase.schema.api.interfaces.converters;

import java.util.Map;
import java.util.NavigableMap;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

/**
 * Interface to extract a map ({@code byte[]} -> {@code Long}) from a Java object
 *
 * @param <T> object type
 */
@FunctionalInterface
public interface HBaseLongMapGetter<T> {
    NavigableMap<byte[], Long> getLongMap(T obj);

    /**
     * Builds a map getter that always returns an empty map
     *
     * @param <T> object type
     * @return map getter instance
     */
    static <T> HBaseLongMapGetter<T> empty() {
        NavigableMap<byte[], Long> result = asBytesTreeMap();
        return obj -> result;
    }

    /**
     * Builds a new {@link HBaseLongMapGetter} object from a long-conversible map field
     *
     * @param getter             lambda to get the field map from the object
     * @param qualifierConverter lambda to convert the key into a {@code byte[]} qualifier
     * @param valueConverter     lambda to convert the key into a {@code Long} value
     * @param <T>                object type
     * @param <K>                key type
     * @param <V>                value type
     * @return long map getter instance
     */
    static <T, K, V> HBaseLongMapGetter<T> longMapGetter(Function<T, ? extends Map<K, V>> getter,
                                                         HBaseBytesGetter<K> qualifierConverter,
                                                         HBaseLongGetter<V> valueConverter) {
        return obj -> {
            NavigableMap<byte[], Long> result = asBytesTreeMap();

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
                result.put(qualifierConverter.getBytes(key), valueConverter.getLong(value));
            }

            return result;
        };
    }

    /**
     * Builds a new {@link HBaseLongMapGetter} object from a long map field
     *
     * @param getter             lambda to get the field map from the object
     * @param qualifierConverter lambda to convert the key into a {@code byte[]} qualifier
     * @param <T>                object type
     * @param <K>                key type
     * @return long map getter instance
     */
    static <T, K> HBaseLongMapGetter<T> longMapGetter(Function<T, ? extends Map<K, Long>> getter,
                                                      HBaseBytesGetter<K> qualifierConverter) {
        return longMapGetter(getter, qualifierConverter, l -> l);
    }
}
