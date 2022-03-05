package hbase.schema.api.converters;

import hbase.schema.api.interfaces.conversion.LongConverter;

import java.time.Instant;

/**
 * Converts Instants to epoch milliseconds
 */
public class InstantLongConverter implements LongConverter<Instant> {
    private static final InstantLongConverter instance = new InstantLongConverter();

    /**
     * Converts the instant to an epoch milliseconds value
     */
    @Override
    public Long toLong(Instant instant) {
        return instant.toEpochMilli();
    }

    /**
     * Creates an instant from an epoch milliseconds value
     */
    @Override
    public Instant fromLong(Long l) {
        return Instant.ofEpochMilli(l);
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
    public static InstantLongConverter instantLongConverter() {
        return instance;
    }
}
