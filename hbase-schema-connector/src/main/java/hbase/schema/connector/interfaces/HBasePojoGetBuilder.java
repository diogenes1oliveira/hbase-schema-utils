package hbase.schema.connector.interfaces;

import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import org.apache.hadoop.hbase.client.Get;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;

import static hbase.schema.api.utils.HBaseSchemaUtils.addFilters;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Interface to effectively build, execute and parse a Get POJO query
 */
@FunctionalInterface
public interface HBasePojoGetBuilder<T> {
    Get toGet(T query);

    default List<Get> toGets(Collection<? extends T> queries) throws IOException {
        return queries.stream().map(this::toGet).collect(toList());
    }

    default Get toGet(T query,
                      byte[] family,
                      HBaseBytesMapper<T> rowKeyGenerator,
                      HBaseReadSchema<T> readSchema) {
        SortedSet<byte[]> qualifiers = readSchema.getQualifiers(query);
        SortedSet<byte[]> qualifierPrefixes = readSchema.getQualifierPrefixes(query);

        byte[] rowKey = requireNonNull(rowKeyGenerator.getBytes(query));
        Get get = new Get(rowKey);
        addFilters(get, family, qualifiers, qualifierPrefixes, readSchema.toFilter(query));
        return get;
    }
}
