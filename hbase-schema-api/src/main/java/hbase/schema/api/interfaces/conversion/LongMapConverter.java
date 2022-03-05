package hbase.schema.api.interfaces.conversion;

import hbase.schema.api.utils.HBaseSchemaUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.Supplier;

import static hbase.schema.api.utils.HBaseSchemaConversions.transformMap;
import static hbase.schema.api.utils.HBaseSchemaConversions.transformMapValues;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

/**
 * Generic interface for conversions of {@code byte[]} Maps
 *
 * @param <T> type to be converted to/from a bytes map
 */
public interface LongMapConverter<T> extends BytesMapConverter<T> {

    /**
     * Transforms a value into a bytes map payload
     *
     * @param value value instance
     * @return corresponding bytes map
     */
    NavigableMap<byte[], Long> toLongMap(T value);

    /**
     * Parses a value from a bytes map payload
     *
     * @param longMap bytes map
     * @return corresponding parsed value
     */
    T fromLongMap(NavigableMap<byte[], Long> longMap);

    @Override
    default NavigableMap<byte[], byte[]> toBytesMap(T value) {
        return transformMapValues(toLongMap(value), asBytesTreeMap(), Bytes::toBytes);
    }

    @Override
    default T fromBytesMap(NavigableMap<byte[], byte[]> bytesMap) {
        NavigableMap<byte[], Long> longMap = transformMapValues(bytesMap, asBytesTreeMap(), Bytes::toLong);
        return fromLongMap(longMap);
    }

    /**
     * Class instance for the value type
     */
    Class<?> type();

    /**
     * A dummy converter for bytes map values
     */
    LongMapConverter<NavigableMap<byte[], Long>> IDENTITY = longMapConverter(
            BytesConverter.identity(), LongConverter.identity(), HBaseSchemaUtils::asBytesTreeMap
    );

    /**
     * A dummy converter for bytes map values
     */
    static LongMapConverter<NavigableMap<byte[], Long>> identity() {
        return IDENTITY;
    }

    /**
     * Creates a new long map converter
     *
     * @param keyConverter   converts the values to/from {@code byte[]}
     * @param valueConverter converts the values to/from {@code Long}
     * @param mapConstructor constructs a new map instance
     * @param <K>            key type
     * @param <V>            value type
     * @param <T>            concrete map type
     * @return new long map converter
     */
    static <K, V, T extends Map<K, V>> LongMapConverter<T> longMapConverter(BytesConverter<K> keyConverter,
                                                                            LongConverter<V> valueConverter,
                                                                            Supplier<T> mapConstructor) {
        return new LongMapConverter<T>() {
            @Override
            public NavigableMap<byte[], Long> toLongMap(T value) {
                return transformMap(value, asBytesTreeMap(), keyConverter::toBytes, valueConverter::toLong);
            }

            @Override
            public T fromLongMap(NavigableMap<byte[], Long> longMap) {
                return transformMap(longMap, mapConstructor.get(), keyConverter::fromBytes, valueConverter::fromLong);
            }

            @Override
            public Class<?> type() {
                return Map.class;
            }
        };
    }

    /**
     * Creates a new long HashMap converter
     *
     * @param keyConverter   converts the values to/from {@code byte[]}
     * @param valueConverter converts the values to/from {@code Long}
     * @param <K>            key type
     * @param <V>            value type
     * @return new long HashMap converter
     */
    static <K, V> LongMapConverter<Map<K, V>> longMapConverter(BytesConverter<K> keyConverter,
                                                               LongConverter<V> valueConverter) {
        return longMapConverter(keyConverter, valueConverter, HashMap::new);
    }

    /**
     * Creates a new long HashMap converter
     *
     * @param keyConverter converts the values to/from {@code byte[]}
     * @param <K>          key type
     * @return new long HashMap converter
     */
    static <K> LongMapConverter<Map<K, Long>> longMapKeyConverter(BytesConverter<K> keyConverter) {
        return longMapConverter(keyConverter, LongConverter.identity());
    }
}
