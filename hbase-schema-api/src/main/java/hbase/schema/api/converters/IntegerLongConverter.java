package hbase.schema.api.converters;

import hbase.schema.api.interfaces.conversion.LongConverter;

/**
 * Converts Boolean instances to 0 and 1 long values
 */
public class IntegerLongConverter implements LongConverter<Integer> {
    private static final IntegerLongConverter instance = new IntegerLongConverter();

    /**
     * Upcasts the integer to a Long
     */
    @Override
    public Long toLong(Integer value) {
        return value.longValue();
    }

    /**
     * Downcasts the integer from a long
     */
    @Override
    public Integer fromLong(Long l) {
        return l.intValue();
    }

    /**
     * {@code Integer.class}
     */
    @Override
    public Class<?> type() {
        return Integer.class;
    }

    /**
     * An instance of this converter to aid in fluent APIs
     */
    public static IntegerLongConverter integerConverter() {
        return instance;
    }
}
