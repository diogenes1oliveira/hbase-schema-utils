package hbase.schema.connector;

import hbase.connector.HBaseConnector;
import hbase.schema.api.interfaces.HBaseCellParser;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.api.interfaces.HBaseWriteSchema;
import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import hbase.schema.api.interfaces.converters.HBaseBytesParser;
import hbase.schema.connector.interfaces.HBasePojoGetBuilder;
import hbase.schema.connector.interfaces.HBasePojoMutationBuilder;
import hbase.schema.connector.interfaces.HBasePojoScanBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;

import static java.util.Objects.requireNonNull;

public class HBaseSchemaConnector<T> implements HBasePojoScanBuilder, HBasePojoGetBuilder, HBasePojoMutationBuilder {
    private final HBaseConnector connector;
    private final TableName tableName;
    private final byte[] family;
    private final HBaseReadSchema<T> readSchema;
    private final HBaseBytesParser<T> rowKeyParser;
    private final HBaseBytesMapper<T> rowKeyGenerator;

    public HBaseSchemaConnector(HBaseReadSchema<T> readSchema,
                                HBaseConnector connector,
                                TableName tableName,
                                byte[] family) {
        this.readSchema = readSchema;
        this.connector = connector;
        this.tableName = tableName;
        this.family = family;

        this.rowKeyParser = readSchema.getRowKeyParser();
        this.rowKeyGenerator = readSchema.getRowKeyGenerator();
        this.qualifiers = readSchema.getQualifiers()
    }

    @Override
    public <T> List<T> get(Collection<? extends T> queries) throws IOException {
        List<Get> gets = this.<T>toGets(queries, family, );

    }

    @Override
    public <T> List<byte[]> mutate(Collection<? extends T> pojos, HBaseWriteSchema<T> writeSchema) throws IOException {
        return null;
    }

    @Override
    public <T> List<T> scan(Collection<? extends T> queries, HBaseReadSchema<T> readSchema) throws IOException {
        return null;
    }


    public <T> void parse(T obj, byte[] rowKey, SortedMap<byte[], byte[]> cells, HBaseReadSchema<T> readSchema) {
        readSchema.getRowKeyParser().setFromBytes(obj, rowKey);
        for (HBaseCellParser<T> parser : readSchema.getCellParsers()) {
            for (Map.Entry<byte[], byte[]> cellEntry : cells.entrySet()) {
                byte[] qualifier = cellEntry.getKey();
                byte[] value = cellEntry.getValue();
                parser.parse(obj, qualifier, value);
            }
        }
    }

    public <T> void parse(T obj, byte[] rowKey, SortedMap<byte[], byte[]> cells, HBaseReadSchema<T> readSchema) {
        readSchema.getRowKeyParser().setFromBytes(obj, rowKey);
        for (HBaseCellParser<T> parser : readSchema.getCellParsers()) {
            for (Map.Entry<byte[], byte[]> cellEntry : cells.entrySet()) {
                byte[] qualifier = cellEntry.getKey();
                byte[] value = cellEntry.getValue();
                parser.parse(obj, qualifier, value);
            }
        }
    }

    public List<Get> toGets(Collection<? extends T> queries) {
        List<Get> gets = new ArrayList<>();
        for (T query : queries) {
            SortedSet<byte[]> qualifiers = readSchema.getQualifiers(query);
            SortedSet<byte[]> qualifierPrefixes = readSchema.getQualifiers(query);

            byte[] rowKey = requireNonNull(rowKeyGenerator.getBytes(query));
            Get get = new Get(rowKey);

            if (qualifierPrefixes.isEmpty()) {
                for (byte[] qualifier : qualifiers) {
                    get.addColumn(family, qualifier);
                }
            } else {
                get.addFamily(family);
            }
            Filter filter = readSchema.toFilter(query);
            if (filter != null) {
                get.setFilter(filter);
            }

            gets.add(get);
        }

        return gets;
    }
}
