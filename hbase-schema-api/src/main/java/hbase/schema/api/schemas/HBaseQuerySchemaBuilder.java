package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.interfaces.conversion.BytesConverter;

import java.nio.charset.StandardCharsets;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseSchemaUtils.*;
import static java.util.Arrays.asList;

/**
 * Builder for {@link HBaseQuerySchema} objects, providing a fluent API to add fields and field prefixes to be fetched
 *
 * @param <T> query object instance
 */
public class HBaseQuerySchemaBuilder<T> {
    private Function<T, byte[]> rowKeyGetter = null;
    private final NavigableSet<byte[]> qualifiers = asBytesTreeSet();
    private final NavigableSet<byte[]> prefixes = asBytesTreeSet();

    /**
     * Generates the row key to be used in a Get request
     *
     * @param getter lambda to build a Get row key from the object
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withRowKey(Function<T, byte[]> getter) {
        this.rowKeyGetter = getter;
        return this;
    }

    /**
     * Generates the row key to be used in a Get request
     *
     * @param getter    lambda to get a row key value from the object
     * @param converter converts the value to a proper {@code byte[]}
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseQuerySchemaBuilder<T> withRowKey(Function<T, U> getter, Function<U, byte[]> converter) {
        return withRowKey(chain(getter, converter));
    }

    /**
     * Generates the row key to be used in a Get request
     *
     * @param getter    lambda to get a row key value from the object
     * @param converter converts the value to a proper {@code byte[]}
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseQuerySchemaBuilder<T> withRowKey(Function<T, U> getter, BytesConverter<U> converter) {
        return withRowKey(getter, converter::toBytes);
    }

    /**
     * Sets the fixed qualifiers to be fetched in the query results
     *
     * @param first fixed {@code byte[]} qualifier
     * @param rest  other qualifiers
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withQualifiers(byte[] first, byte[]... rest) {
        qualifiers.add(first);
        qualifiers.addAll(asList(rest));

        return this;
    }

    /**
     * Sets the fixed qualifiers to be fetched in the query results
     *
     * @param first fixed UTF-8 qualifier
     * @param rest  other qualifiers
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withQualifiers(String first, String... rest) {
        return withQualifiers(
                first.getBytes(StandardCharsets.UTF_8),
                mapArray(rest, byte[].class, s -> s.getBytes(StandardCharsets.UTF_8))
        );
    }

    /**
     * Sets the qualifier prefixes to be fetched in the query results
     *
     * @param first fixed {@code byte[]} prefix
     * @param rest  other prefixes
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withPrefixes(byte[] first, byte[]... rest) {
        prefixes.add(first);
        prefixes.addAll(asList(rest));

        return this;
    }

    /**
     * Sets the qualifier prefixes to be fetched in the query results
     *
     * @param first fixed UTF-8 prefix
     * @param rest  other prefixes
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withPrefixes(String first, String... rest) {
        return withPrefixes(
                first.getBytes(StandardCharsets.UTF_8),
                mapArray(rest, byte[].class, s -> s.getBytes(StandardCharsets.UTF_8))
        );
    }

    /**
     * Builds a new instance of the query schema
     *
     * @return new query schema instance
     */
    public HBaseQuerySchema<T> build() {
        verifyNonNull("no row key generator is set", rowKeyGetter);
        verifyNonEmpty("needs qualifiers or prefixes", qualifiers, prefixes);

        return new HBaseQuerySchema<T>() {
            @Override
            public byte[] buildRowKey(T query) {
                return rowKeyGetter.apply(query);
            }

            @Override
            public SortedSet<byte[]> getQualifiers(T query) {
                return qualifiers;
            }

            @Override
            public SortedSet<byte[]> getPrefixes(T query) {
                return prefixes;
            }
        };
    }

}
