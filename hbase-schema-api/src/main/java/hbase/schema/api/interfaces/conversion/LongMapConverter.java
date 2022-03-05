package hbase.schema.api.interfaces.conversion;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

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
    LongMapConverter<NavigableMap<byte[], Long>> IDENTITY = new LongMapConverter<NavigableMap<byte[], Long>>() {
        @Override
        public NavigableMap<byte[], Long> toLongMap(NavigableMap<byte[], Long> longMap) {
            return longMap;
        }

        @Override
        public NavigableMap<byte[], Long> fromLongMap(NavigableMap<byte[], Long> longMap) {
            return longMap;
        }

        @Override
        public Class<?> type() {
            return Map.class;
        }
    };

    /**
     * A dummy converter for bytes map values
     */
    static LongMapConverter<NavigableMap<byte[], Long>> identity() {
        return IDENTITY;
    }

    static <K, V> LongMapConverter<Map<K, V>> longMapConverter(BytesConverter<K> keyConverter,
                                                               LongConverter<V> valueConverter) {
        return new LongMapConverter<Map<K, V>>() {
            @Override
            public NavigableMap<byte[], Long> toLongMap(Map<K, V> value) {
                return transformMap(value, asBytesTreeMap(), keyConverter::toBytes, valueConverter::toLong);
            }

            @Override
            public Map<K, V> fromLongMap(NavigableMap<byte[], Long> longMap) {
                return transformMap(longMap, new HashMap<>(), keyConverter::fromBytes, valueConverter::fromLong);
            }

            @Override
            public Class<?> type() {
                return Map.class;
            }
        };
    }
}
