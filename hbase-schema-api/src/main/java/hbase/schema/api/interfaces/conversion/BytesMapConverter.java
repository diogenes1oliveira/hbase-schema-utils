package hbase.schema.api.interfaces.conversion;

import hbase.schema.api.utils.HBaseSchemaUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.Supplier;

import static hbase.schema.api.utils.HBaseSchemaConversions.transformMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

/**
 * Generic interface for conversions of {@code byte[]} Maps
 *
 * @param <T> type to be converted to/from a bytes map
 */
public interface BytesMapConverter<T> {

    /**
     * A dummy converter for bytes map values
     */
    BytesMapConverter<NavigableMap<byte[], byte[]>> IDENTITY = bytesMapConverter(
            BytesConverter.bytesConverter(), BytesConverter.bytesConverter(), HBaseSchemaUtils::asBytesTreeMap
    );

    /**
     * Transforms a value into a bytes map payload
     *
     * @param value value instance
     * @return corresponding bytes map
     */
    NavigableMap<byte[], byte[]> toBytesMap(T value);

    /**
     * Parses a value from a bytes map payload
     *
     * @param bytesMap bytes map
     * @return corresponding parsed value
     */
    T fromBytesMap(NavigableMap<byte[], byte[]> bytesMap);

    /**
     * Class instance for the value type
     */
    Class<?> type();

    /**
     * A dummy converter for bytes map values
     */
    static BytesMapConverter<NavigableMap<byte[], byte[]>> bytesMapConverter() {
        return IDENTITY;
    }

    /**
     * Creates a new bytes map converter
     *
     * @param keyConverter   converts the values to/from {@code byte[]}
     * @param valueConverter converts the values to/from {@code byte[]}
     * @param mapConstructor constructs a new map instance
     * @param <K>            key type
     * @param <V>            value type
     * @param <T>            concrete map type
     * @return new long map converter
     */
    static <K, V, T extends Map<K, V>> BytesMapConverter<T> bytesMapConverter(BytesConverter<K> keyConverter,
                                                                              BytesConverter<V> valueConverter,
                                                                              Supplier<T> mapConstructor) {
        return new BytesMapConverter<T>() {
            @Override
            public NavigableMap<byte[], byte[]> toBytesMap(T value) {
                return transformMap(value, asBytesTreeMap(), keyConverter::toBytes, valueConverter::toBytes);
            }

            @Override
            public T fromBytesMap(NavigableMap<byte[], byte[]> bytesMap) {
                return transformMap(bytesMap, mapConstructor.get(), keyConverter::fromBytes, valueConverter::fromBytes);
            }

            @Override
            public Class<?> type() {
                return Map.class;
            }
        };
    }

    /**
     * Creates a new bytes HashMap converter
     *
     * @param keyConverter   converts the values to/from {@code byte[]}
     * @param valueConverter converts the values to/from {@code byte[]}
     * @param <K>            key type
     * @param <V>            value type
     * @return new bytes HashMap converter
     */
    static <K, V> BytesMapConverter<Map<K, V>> bytesMapConverter(BytesConverter<K> keyConverter,
                                                                 BytesConverter<V> valueConverter) {
        return bytesMapConverter(keyConverter, valueConverter, HashMap::new);
    }

}
