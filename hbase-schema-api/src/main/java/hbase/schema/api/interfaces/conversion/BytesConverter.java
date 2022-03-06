package hbase.schema.api.interfaces.conversion;

import java.util.function.Function;

/**
 * Generic interface for {@code byte[]} conversions
 *
 * @param <T> type to be converted to/from bytes
 */
public interface BytesConverter<T> {
    /**
     * A dummy converter for {@code byte[]} values
     */
    BytesConverter<byte[]> IDENTITY = new BytesConverter<byte[]>() {
        @Override
        public byte[] toBytes(byte[] value) {
            return value;
        }

        @Override
        public byte[] fromBytes(byte[] bytes) {
            return bytes;
        }

        @Override
        public Class<?> type() {
            return byte[].class;
        }
    };

    /**
     * Transforms a value into a {@code byte[]} payload
     *
     * @param value value instance
     * @return corresponding {@code byte[]} value
     */
    byte[] toBytes(T value);

    /**
     * Parses a value from a {@code byte[]} payload
     *
     * @param bytes binary payload
     * @return corresponding parsed value
     */
    T fromBytes(byte[] bytes);

    /**
     * Class instance for the value type
     */
    Class<?> type();

    /**
     * A dummy converter for {@code byte[]} values
     */
    static BytesConverter<byte[]> bytesConverter() {
        return IDENTITY;
    }

    /**
     * Creates a new converter from functional lambdas
     *
     * @param toBytes   lambda to convert the value into {@code byte[]}
     * @param fromBytes lambda to parse the value from {@code byte[]}
     * @param type      class instance
     * @param <T>       value type
     * @return new bytes converter
     */
    static <T> BytesConverter<T> bytesConverter(Function<T, byte[]> toBytes,
                                                Function<byte[], T> fromBytes,
                                                Class<T> type) {
        return new BytesConverter<T>() {
            @Override
            public byte[] toBytes(T value) {
                return toBytes.apply(value);
            }

            @Override
            public T fromBytes(byte[] bytes) {
                return fromBytes.apply(bytes);
            }

            @Override
            public Class<?> type() {
                return type;
            }
        };
    }
}
