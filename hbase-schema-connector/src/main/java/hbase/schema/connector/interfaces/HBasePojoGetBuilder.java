package hbase.schema.connector.interfaces;

import edu.umd.cs.findbugs.annotations.Nullable;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.connector.HBaseSchemaUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;

import static java.util.stream.Collectors.toList;

/**
 * Interface to effectively build, execute and parse a Get POJO query
 */
public interface HBasePojoGetBuilder<T> {
    Get toGet(T query);

    Optional<T> get(Get get) throws IOException;

    default List<Get> toGets(Collection<? extends T> queries) {
        return queries.stream().map(this::toGet).collect(toList());
    }

    default void selectColumns(Get get, T query, byte[] family, HBaseReadSchema<T> readSchema) {
        SortedSet<byte[]> qualifiers = readSchema.getQualifiers(query);
        SortedSet<byte[]> qualifierPrefixes = readSchema.getQualifierPrefixes(query);

        HBaseSchemaUtils.selectColumns(get, family, qualifiers, qualifierPrefixes);
    }

    @Nullable
    default Filter toFilter(T query, HBaseReadSchema<T> readSchema) {
        return readSchema.toFilter(query);
    }
}
