package hbase.schema.api.interfaces.conversion;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.function.Function;

/**
 * Generic interface for {@code Long} conversions
 *
 * @param <T> type to be converted to/from bytes
 */
public interface LongConverter<T> extends BytesConverter<T> {
    /**
     * A dummy converter for {@code Long} values
     */
    LongConverter<Long> IDENTITY = new LongConverter<Long>() {
        @Override
        public Long toLong(Long value) {
            return value;
        }

        @Override
        public Long fromLong(Long value) {
            return value;
        }

        @Override
        public Class<?> type() {
            return Long.class;
        }
    };

    /**
     * Transforms a value into a {@code Long} payload
     *
     * @param value value instance
     * @return corresponding {@code Long} value
     */
    Long toLong(T value);

    /**
     * Parses a value from a {@code Long} payload
     *
     * @param l long value
     * @return corresponding parsed value
     */
    T fromLong(Long l);

    /**
     * Encodes the Long as a big-endian byte array
     */
    @Override
    default byte[] toBytes(T value) {
        long l = toLong(value);
        return Bytes.toBytes(l);
    }

    /**
     * Decodes the Long from a big-endian byte array
     */
    @Override
    default T fromBytes(byte[] bytes) {
        long l = Bytes.toLong(bytes);
        return fromLong(l);
    }

    /**
     * Class instance for the value type
     */
    Class<?> type();

    /**
     * A dummy converter for {@code Long} values
     */
    static LongConverter<Long> longConverter() {
        return IDENTITY;
    }

    /**
     * Creates a new converter from functional lambdas
     *
     * @param toLong   lambda to convert the value into {@code Long}
     * @param fromLong lambda to parse the value from {@code Long}
     * @param type     class instance
     * @param <T>      value type
     * @return new long converter
     */
    static <T> LongConverter<T> longConverter(Function<T, Long> toLong,
                                              Function<Long, T> fromLong,
                                              Class<T> type) {
        return new LongConverter<T>() {
            @Override
            public Long toLong(T value) {
                return toLong.apply(value);
            }

            @Override
            public T fromLong(Long bytes) {
                return fromLong.apply(bytes);
            }

            @Override
            public Class<?> type() {
                return type;
            }
        };
    }
}
