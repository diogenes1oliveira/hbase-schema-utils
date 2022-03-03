package hbase.schema.api.interfaces.converters;

import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Comparator.naturalOrder;

/**
 * Interface to populate a Java object with data parsed from a map ({@code byte[]} -> {@code byte[]})
 *
 * @param <T> object type
 */
@FunctionalInterface
public interface HBaseBytesMapSetter<T> {
    void setFromBytes(T obj, NavigableMap<byte[], byte[]> bytesMap);

    /**
     * Builds a new {@link HBaseBytesMapSetter} object from a bytes-conversible map field
     *
     * @param setter             lambda to set the field map into the object
     * @param qualifierConverter lambda to convert the key into a {@code byte[]} qualifier
     * @param valueConverter     lambda to convert the key into a {@code byte[]} value
     * @param keyComparator      object to compare keys
     * @param <T>                object type
     * @param <K>                key type
     * @param <V>                value type
     * @return bytes map getter instance
     */
    static <T, K, V> HBaseBytesMapSetter<T> bytesMapSetter(BiConsumer<T, Map<K, V>> setter,
                                                           Function<byte[], K> qualifierConverter,
                                                           Function<byte[], V> valueConverter,
                                                           Comparator<K> keyComparator) {
        return (obj, bytesMap) -> {
            TreeMap<K, V> result = new TreeMap<>(keyComparator);

            for (Map.Entry<byte[], byte[]> entry : bytesMap.entrySet()) {
                K key = qualifierConverter.apply(entry.getKey());
                V value = valueConverter.apply(entry.getValue());
                if (key == null || value == null) {
                    continue;
                }
                result.put(key, value);
            }

            setter.accept(obj, result);
        };
    }

    /**
     * Builds a new {@link HBaseBytesMapSetter} object from a bytes-conversible map field
     *
     * @param setter             lambda to set the field map into the object
     * @param qualifierConverter lambda to convert the key into a {@code byte[]} qualifier
     * @param valueConverter     lambda to convert the key into a {@code byte[]} value
     * @param <T>                object type
     * @param <K>                key type
     * @param <V>                value type
     * @return bytes map getter instance
     */
    static <T, K extends Comparable<K>, V> HBaseBytesMapSetter<T> bytesMapSetter(BiConsumer<T, Map<K, V>> setter,
                                                                                 Function<byte[], K> qualifierConverter,
                                                                                 Function<byte[], V> valueConverter) {
        return bytesMapSetter(setter, qualifierConverter, valueConverter, naturalOrder());
    }
}
