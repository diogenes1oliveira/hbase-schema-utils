package hbase.schema.api.converters;

import hbase.schema.api.interfaces.conversion.BytesConverter;

import java.sql.Timestamp;

import static hbase.schema.api.converters.InstantStringConverter.instantStringConverter;

/**
 * Converts Timestamps by parsing/reading from an ISO string
 */
public class TimestampStringConverter implements BytesConverter<Timestamp> {
    private static final InstantStringConverter instantConverter = instantStringConverter();
    private static final TimestampStringConverter instance = new TimestampStringConverter();

    /**
     * Converts the timestamp to a UTF-8 encoded ISO string
     */
    @Override
    public byte[] toBytes(Timestamp timestamp) {
        return instantConverter.toBytes(timestamp.toInstant());
    }

    /**
     * Parses the timestamp from a UTF-8 encoded ISO string
     */
    @Override
    public Timestamp fromBytes(byte[] bytes) {
        return Timestamp.from(instantConverter.fromBytes(bytes));
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
    public static TimestampStringConverter timestampStringConverter() {
        return instance;
    }
}
