package hbase.schema.api.interfaces.conversion;

import java.util.NavigableMap;

/**
 * Generic interface for conversions of {@code byte[]} Maps
 *
 * @param <T> type to be converted to/from a bytes map
 */
public interface BytesMapConverter<T> {

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
    BytesMapConverter<NavigableMap<byte[], byte[]>> IDENTITY = new BytesMapConverter<NavigableMap<byte[], byte[]>>() {
        @Override
        public NavigableMap<byte[], byte[]> toBytesMap(NavigableMap<byte[], byte[]> bytesMap) {
            return bytesMap;
        }

        @Override
        public NavigableMap<byte[], byte[]> fromBytesMap(NavigableMap<byte[], byte[]> bytesMap) {
            return bytesMap;
        }

        @Override
        public Class<?> type() {
            return NavigableMap.class;
        }
    };

    /**
     * A dummy converter for bytes map values
     */
    static BytesMapConverter<NavigableMap<byte[], byte[]>> bytesMapConverter() {
        return IDENTITY;
    }

}
