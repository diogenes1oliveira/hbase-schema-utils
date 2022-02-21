package hbase.schema.api.interfaces;

import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import hbase.schema.api.interfaces.converters.HBaseBytesParser;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;

import static hbase.schema.api.utils.HBaseSchemaUtils.addFilters;
import static java.util.Objects.requireNonNull;

/**
 * Interface to generate Get queries corresponding to a POJO query
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseGetGenerator<T> {
    List<Get> toGets(Collection<? extends T> queries, byte[] family);

    /**
     * Builds a Get request based on the data from a POJO object
     *
     * @param query      POJO object to act as query source data
     * @param readSchema object to generate the Get data from the POJO
     * @return built Get request
     */
    default Get toGet(T query,
                      byte[] family,
                      HBaseBytesMapper<T> rowKeyGenerator,
                      HBaseReadSchema<T> readSchema,
                      ) {
        SortedSet<byte[]> qualifiers = readSchema.getQualifiers(query);
        SortedSet<byte[]> qualifierPrefixes = readSchema.getQualifiers(query);

        byte[] rowKey = requireNonNull(rowKeyGenerator.getBytes(query));
        Get get = new Get(rowKey);
        addFilters(get, family, qualifiers, qualifierPrefixes, readSchema.toFilter(query));
        return get;
    }

}
