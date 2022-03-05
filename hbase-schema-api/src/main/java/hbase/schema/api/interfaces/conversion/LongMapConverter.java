package hbase.schema.api.interfaces.conversion;

import hbase.schema.api.utils.HBaseSchemaUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.NavigableMap;

import static hbase.schema.api.utils.HBaseSchemaConversions.mapValues;
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
        return mapValues.<byte[], byte[], Long>(toLongMap(value), asBytesTreeMap(), );
    }

    @Override
    default T fromBytesMap(NavigableMap<byte[], byte[]> bytesMap) {
        return null;
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
            return NavigableMap.class;
        }
    };

    /**
     * A dummy converter for bytes map values
     */
    static LongMapConverter<NavigableMap<byte[], Long>> longMapConverter() {
        return IDENTITY;
    }

}
