package hbase.schema.api.converters;

import hbase.schema.api.interfaces.conversion.LongConverter;

/**
 * Converts Boolean instances to 0 and 1 long values
 */
public class BooleanLongConverter implements LongConverter<Boolean> {
    private static final BooleanLongConverter instance = new BooleanLongConverter();

    /**
     * Returns 0 for {@code true} and 1 for {@code false}
     */
    @Override
    public Long toLong(Boolean value) {
        return Boolean.TRUE.equals(value) ? 1L : 0L;
    }

    /**
     * Returns {@code true} for non-zero longs and {@code false} otherwise
     */
    @Override
    public Boolean fromLong(Long l) {
        return l != 0L;
    }

    /**
     * {@code Boolean.class}
     */
    @Override
    public Class<?> type() {
        return Boolean.class;
    }

    /**
     * An instance of this converter to aid in fluent APIs
     */
    public static BooleanLongConverter booleanLongConverter() {
        return instance;
    }
}
