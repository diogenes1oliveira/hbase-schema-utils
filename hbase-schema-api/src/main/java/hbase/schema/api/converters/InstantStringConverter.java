package hbase.schema.api.converters;

import hbase.schema.api.interfaces.conversion.BytesConverter;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Converts Instants by parsing/reading from an ISO string
 */
public class InstantStringConverter implements BytesConverter<Instant> {
    private static final InstantStringConverter instance = new InstantStringConverter();

    /**
     * Converts the instant to a UTF-8 encoded ISO string
     */
    @Override
    public byte[] toBytes(Instant instant) {
        return instant.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses the instant from a UTF-8 encoded ISO string
     */
    @Override
    public Instant fromBytes(byte[] bytes) {
        try {
            return Instant.parse(new String(bytes, StandardCharsets.UTF_8));
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * {@code Instant.class}
     */
    @Override
    public Class<?> type() {
        return Instant.class;
    }

    /**
     * An instance of this converter to aid in fluent APIs
     */
    public static InstantStringConverter instantStringConverter() {
        return instance;
    }
}
