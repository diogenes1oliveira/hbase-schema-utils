package hbase.schema.api.converters;

import hbase.schema.api.interfaces.conversion.BytesConverter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.format.DateTimeParseException;

/**
 * Converts Instants by parsing/reading from an ISO string
 */
public class DurationStringConverter implements BytesConverter<Duration> {
    private static final DurationStringConverter instance = new DurationStringConverter();

    /**
     * Converts the instant to a UTF-8 encoded ISO string
     */
    @Override
    public byte[] toBytes(Duration instant) {
        return instant.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses the instant from a UTF-8 encoded ISO string
     */
    @Override
    public Duration fromBytes(byte[] bytes) {
        try {
            return Duration.parse(new String(bytes, StandardCharsets.UTF_8));
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * {@code Duration.class}
     */
    @Override
    public Class<?> type() {
        return Duration.class;
    }

    /**
     * An instance of this converter to aid in fluent APIs
     */
    public static DurationStringConverter durationStringConverter() {
        return instance;
    }
}
