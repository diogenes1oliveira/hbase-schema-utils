package hbase.schema.connector.interfaces;

import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.connector.HBaseSchemaUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;

import static hbase.schema.connector.HBaseSchemaUtils.toMultiRowRangeFilter;

/**
 * Interface to effectively build, execute and parse a Scan POJO query
 */
@FunctionalInterface
public interface HBasePojoScanBuilder<T> {
    Scan toScan(List<? extends T> queries) throws IOException;

    default void selectColumns(Scan scan, T query, byte[] family, HBaseReadSchema<T> readSchema) {
        SortedSet<byte[]> qualifiers = readSchema.getQualifiers(query);
        SortedSet<byte[]> qualifierPrefixes = readSchema.getQualifierPrefixes(query);

        HBaseSchemaUtils.selectColumns(scan, family, qualifiers, qualifierPrefixes);
    }

    default Filter toFilter(List<? extends T> queries, byte[] family, HBaseReadSchema<T> readSchema) {
        Filter scanFilter = toMultiRowRangeFilter(queries
                .stream()
                .map(readSchema.getScanRowKeyGenerator()::getBytes)
                .iterator()
        );
        Filter schemaFilter = readSchema.toFilter(queries.get(0));
        if (schemaFilter != null) {
            return new FilterList(scanFilter, schemaFilter);
        } else {
            return scanFilter;
        }
    }

}
