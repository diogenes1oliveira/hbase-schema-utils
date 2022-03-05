package hbase.schema.api.converters;

import hbase.schema.api.interfaces.conversion.LongConverter;

import java.sql.Timestamp;

/**
 * Converts Timestamps to epoch milliseconds
 */
public class TimestampLongConverter implements LongConverter<Timestamp> {
    private static final TimestampLongConverter instance = new TimestampLongConverter();

    /**
     * Converts the timestamp to an epoch milliseconds value
     */
    @Override
    public Long toLong(Timestamp timestamp) {
        return timestamp.getTime();
    }

    /**
     * Creates a timestamp from an epoch milliseconds value
     */
    @Override
    public Timestamp fromLong(Long l) {
        return new Timestamp(l);
    }

    /**
     * {@code Timestamp.class}
     */
    @Override
    public Class<?> type() {
        return Timestamp.class;
    }

    /**
     * An instance of this converter to aid in fluent APIs
     */
    public static TimestampLongConverter timestampLongConverter() {
        return instance;
    }
}
