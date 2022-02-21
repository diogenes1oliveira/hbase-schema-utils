package hbase.schema.connector.interfaces;

import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;

import static hbase.schema.api.utils.HBaseSchemaUtils.addFilters;
import static hbase.schema.api.utils.HBaseSchemaUtils.toMultiRowRangeFilter;
import static java.util.Objects.requireNonNull;

/**
 * Interface to effectively build, execute and parse a Scan POJO query
 */
@FunctionalInterface
public interface HBasePojoScanBuilder<T> {
    Scan toScan(List<? extends T> queries) throws IOException;

    default Scan toScan(List<? extends T> queries,
                        byte[] family,
                        HBaseBytesMapper<T> scanRowKeyGenerator,
                        HBaseReadSchema<T> readSchema) {
        if (queries.size() == 0) {
            throw new IllegalArgumentException("At least one query is required");
        }
        SortedSet<byte[]> qualifiers = readSchema.getQualifiers(queries.get(0));
        SortedSet<byte[]> qualifierPrefixes = readSchema.getQualifierPrefixes(queries.get(0));
        MultiRowRangeFilter rowRangeFilter = toMultiRowRangeFilter(
                queries.stream().map(scanRowKeyGenerator::getBytes).iterator()
        );
        Filter
        byte[] rowKey = requireNonNull(rowKeyGenerator.getBytes(query));
        Scan scan = new Scan();
        addFilters(scan, family, qualifiers, qualifierPrefixes, null);
        return get;
    }

    default Filter getScanFilter(List<? extends T> queries, HBaseReadSchema<T> readSchema) {
        return toMultiRowRangeFilter(
                queries.stream().map(readSchema.getRowKeyGenerator()::getBytes).iterator()
        );
    }
}
