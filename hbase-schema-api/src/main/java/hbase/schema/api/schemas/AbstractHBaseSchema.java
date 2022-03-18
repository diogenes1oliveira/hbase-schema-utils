package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseFilterSchema;
import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.interfaces.HBaseResultParserSchema;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.api.interfaces.conversion.BytesConverter;
import hbase.schema.api.interfaces.conversion.BytesMapConverter;
import hbase.schema.api.interfaces.conversion.LongConverter;
import hbase.schema.api.interfaces.conversion.LongMapConverter;
import org.apache.hadoop.hbase.filter.Filter;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Base director class to build a schema from getters and setters
 *
 * @param <T> mutation and query input type
 * @param <R> result type
 */
@SuppressWarnings("SameParameterValue")
public abstract class AbstractHBaseSchema<T, R> implements HBaseSchema<T, T, R> {
    private final HBaseMutationSchemaBuilder<T> mutationBuilder = new HBaseMutationSchemaBuilder<T>()
            .withTimestamp(this::buildTimestamp)
            .withRowKey(this::buildRowKey);
    private final HBaseQuerySchemaBuilder<T> queryBuilder = new HBaseQuerySchemaBuilder<T>()
            .withRowKey(this::buildRowKey);
    private final HBaseFilterSchemaBuilder<T> filterBuilder = new HBaseFilterSchemaBuilder<T>()
            .withFilter(this::buildFilter);
    private final HBaseResultParserSchemaBuilder<R> resultBuilder = new HBaseResultParserSchemaBuilder<>(this::newInstance)
            .fromRowKey(this::parseRowKey);
    private final Map<String, Function<T, Long>> timestampers = new HashMap<>();

    /**
     * Creates a new output instance
     */
    public abstract R newInstance();

    /**
     * Calculates a global timestamp for the object
     *
     * @param object input object
     * @return timestamp in milliseconds
     */
    public abstract @Nullable Long buildTimestamp(T object);

    /**
     * Calculates a timestamp for a specific field
     *
     * @param object input object
     * @param field  field name
     * @return timestamp in milliseconds
     */
    public @Nullable Long buildTimestamp(T object, String field) {
        Function<T, Long> timestamper = timestampers.get(field);
        if (timestamper != null) {
            return timestamper.apply(object);
        } else {
            return buildTimestamp(object);
        }
    }

    /**
     * Builds a row key for the input object
     *
     * @param object input object
     * @return row key in {@code byte[]}
     */
    public abstract byte @Nullable [] buildRowKey(T object);

    /**
     * Populates the output object with data from the row key
     * <p>
     * The default implementation does nothing
     *
     * @param object output object
     * @param rowKey row key value
     */
    public void parseRowKey(R object, byte[] rowKey) {
        // nothing to do
    }

    /**
     * Returns the scan key size
     */
    public abstract int scanKeySize();

    /**
     * Builds a new Filter for the query
     * <p>
     * The default implementation returns null
     *
     * @param query query object
     * @return built Filter or null
     */
    @Nullable
    public Filter buildFilter(T query) {
        return null;
    }

    /**
     * Schema to generate the Mutations
     */
    @Override
    public final HBaseMutationSchema<T> mutationSchema() {
        return mutationBuilder.build();
    }

    /**
     * Schema to generate the Gets and Scans queries
     */
    @Override
    public final HBaseQuerySchema<T> querySchema() {
        return queryBuilder.build();
    }

    /**
     * Schema to generate the Filters
     */
    @Override
    public final HBaseFilterSchema<T> filterSchema() {
        return filterBuilder.withScanKey(this::buildRowKey, scanKeySize()).build();
    }

    /**
     * Schema to parse fetched Results
     */
    @Override
    public final HBaseResultParserSchema<R> resultParserSchema() {
        return resultBuilder.build();
    }

    /**
     * Adds a field corresponding to a single HBase value cell
     *
     * @param field     field name
     * @param getter    gets the field value
     * @param setter    sets the field value
     * @param converter converts the field to/from {@code byte[]}
     * @param <F>       field type
     */
    protected <F> void withValue(String field,
                                 @Nullable Function<T, F> getter,
                                 @Nullable BiConsumer<R, F> setter,
                                 @Nullable BytesConverter<F> converter) {
        withValue(field, getter, setter, converter, null);
    }

    /**
     * Adds a field corresponding to a single HBase value cell
     *
     * @param field       field name
     * @param getter      gets the field value
     * @param setter      sets the field value
     * @param converter   converts the field to/from {@code byte[]}
     * @param timestamper extracts a timestamp for the cell
     * @param <F>         field type
     */
    protected <F> void withValue(String field,
                                 @Nullable Function<T, F> getter,
                                 @Nullable BiConsumer<R, F> setter,
                                 BytesConverter<F> converter,
                                 @Nullable Function<T, Long> timestamper) {
        if(getter != null) {
            mutationBuilder.withTimestamp(obj -> buildTimestamp(obj, field)).withValue(field, getter, converter);
        }
        if(setter != null) {
            queryBuilder.withQualifiers(field);
            filterBuilder.withPrefixes(field);
            resultBuilder.fromColumn(field, setter, converter);
        }
        if(timestamper != null) {
            timestampers.put(field, timestamper);
        }
    }

    /**
     * Adds a field corresponding to a single HBase Increment cell
     *
     * @param field     field name
     * @param getter    gets the field value
     * @param setter    sets the field value
     * @param converter converts the field to/from {@code byte[]}
     * @param <F>       field type
     */
    protected <F> void withDelta(String field,
                                 @Nullable Function<T, F> getter,
                                 @Nullable BiConsumer<R, F> setter,
                                 LongConverter<F> converter) {
        if(getter != null) {
            mutationBuilder.withTimestamp(obj -> buildTimestamp(obj, field)).withDelta(field, getter, converter);
        }
        if(setter != null) {
            queryBuilder.withQualifiers(field);
            filterBuilder.withPrefixes(field);
            resultBuilder.fromColumn(field, setter, converter);
        }
    }

    /**
     * Adds a field corresponding to a single HBase value cell
     *
     * @param field  field name
     * @param getter gets the field {@code byte[]} value
     * @param setter sets the field {@code byte[]} value
     */
    protected void withValue(String field,
                             @Nullable Function<T, byte[]> getter,
                             @Nullable BiConsumer<R, byte[]> setter) {
        withValue(field, getter, setter, BytesConverter.bytesConverter());
    }

    /**
     * Adds a field corresponding to a single HBase Increment cell
     *
     * @param field  field name
     * @param getter gets the field {@code Long} value
     * @param setter sets the field {@code Long} value
     */
    protected void withDelta(String field,
                             @Nullable Function<T, Long> getter,
                             @Nullable BiConsumer<R, Long> setter) {
        withDelta(field, getter, setter, LongConverter.longConverter());
    }

    /**
     * Adds a field corresponding to a multiple HBase cells
     *
     * @param prefix    qualifier prefix
     * @param getter    gets the field value
     * @param setter    sets the field value
     * @param converter converts the field to/from a bytes map
     * @param <F>       field type
     */
    protected <F> void withValues(String prefix,
                                  @Nullable Function<T, F> getter,
                                  @Nullable BiConsumer<R, F> setter,
                                  BytesMapConverter<F> converter) {
        if (getter != null) {
            mutationBuilder.withTimestamp(obj -> buildTimestamp(obj, prefix)).withValues(prefix, getter, converter);
        }
        if (setter != null) {
            queryBuilder.withPrefixes(prefix);
            filterBuilder.withPrefixes(prefix);
            resultBuilder.fromPrefix(prefix, setter, converter);
        }
    }

    /**
     * Adds a field corresponding to multiple HBase increments
     *
     * @param prefix    qualifier prefix
     * @param getter    gets the field value
     * @param setter    sets the field value
     * @param converter converts the field to/from a long map
     * @param <F>       field type
     */
    protected <F> void withDeltas(String prefix,
                                  @Nullable Function<T, F> getter,
                                  @Nullable BiConsumer<R, F> setter,
                                  LongMapConverter<F> converter) {
        if (getter != null) {
            mutationBuilder.withTimestamp(obj -> buildTimestamp(obj, prefix)).withValues(prefix, getter, converter);
        }
        if (setter != null) {
            queryBuilder.withPrefixes(prefix);
            filterBuilder.withPrefixes(prefix);
            resultBuilder.fromPrefix(prefix, setter, converter);
        }
    }
}
