package hbase.schema.api.converters;

import hbase.schema.api.interfaces.conversion.BytesConverter;

import java.nio.charset.StandardCharsets;

/**
 * Converts strings by encoding/decoding as UTF-8
 */
public class Utf8Converter implements BytesConverter<String> {
    private static final Utf8Converter instance = new Utf8Converter();

    /**
     * Encodes the string as UTF-8
     */
    @Override
    public byte[] toBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Decodes the string from UTF-8
     */
    @Override
    public String fromBytes(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * {@code String.class}
     */
    @Override
    public Class<?> type() {
        return String.class;
    }

    /**
     * An instance of this converter to aid in fluent APIs
     */
    public static Utf8Converter utf8Converter() {
        return instance;
    }
}
