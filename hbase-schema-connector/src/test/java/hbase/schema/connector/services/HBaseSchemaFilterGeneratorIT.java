package hbase.schema.connector.services;

import hbase.connector.services.HBaseConnector;
import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.schemas.HBaseQuerySchemaBuilder;
import hbase.test.utils.HBaseTestJunitExtension;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import testutils.DummyPojo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

import static hbase.schema.api.utils.HBaseSchemaConversions.stringGetter;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8ToBytes;
import static hbase.schema.api.utils.HBaseSchemaUtils.asStringMap;
import static hbase.test.utils.HBaseTestHelpers.createTable;
import static hbase.test.utils.HBaseTestHelpers.newTableDescriptor;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static testutils.HBaseSchemaConnectorTestHelpers.bytesToStringMap;

@ExtendWith(HBaseTestJunitExtension.class)
class HBaseSchemaFilterGeneratorIT {
    byte[] family = new byte[]{'f'};
    TableName tempTable;
    HBaseConnector connector;

    HBaseQuerySchema<DummyPojo> prefixQuerySchema = new HBaseQuerySchemaBuilder<DummyPojo>()
            .withRowKey(stringGetter(DummyPojo::getId))
            .withScanKeySize(4)
            .withPrefixes("p-")
            .build();

    HBaseSchemaFilterGenerator<DummyPojo> prefixFilterGenerator = new HBaseSchemaFilterGenerator<>(family, prefixQuerySchema);

    @BeforeEach
    void setUp(TableName tempTable, Properties props, Connection connection) {
        this.tempTable = tempTable;
        this.connector = new HBaseConnector(props);

        createTable(connection, newTableDescriptor(tempTable, family));
    }

    @Test
    void testPrefixParser() throws IOException {
        // given a table with three rows with values
        Put put1 = new Put(utf8ToBytes("row1-values"))
                .addColumn(family, utf8ToBytes("p-1"), utf8ToBytes("a"))
                .addColumn(family, utf8ToBytes("p-2"), utf8ToBytes("b"))
                .addColumn(family, utf8ToBytes("dummy"), utf8ToBytes("dummy"));
        Put put2 = new Put(utf8ToBytes("row2-values"))
                .addColumn(family, utf8ToBytes("p-1"), utf8ToBytes("one"))
                .addColumn(family, utf8ToBytes("dummy"), utf8ToBytes("dummy"));
        Put put3 = new Put(utf8ToBytes("row3-values"))
                .addColumn(family, utf8ToBytes("dummy"), utf8ToBytes("dummy"));

        try (Connection connection = connector.context();
             Table table = connection.getTable(tempTable)) {
            table.put(asList(put1, put2, put3));
        }

        DummyPojo query1 = new DummyPojo().withId("row1-query");
        DummyPojo query2 = new DummyPojo().withId("row2-query");
        Filter filter = prefixFilterGenerator.toFilter(asList(query1, query2));
        Scan scan = new Scan().setFilter(filter);
        prefixFilterGenerator.selectColumns(query1, scan);
        List<TreeMap<String, String>> results = new ArrayList<>();

        try (Connection connection = connector.context();
             Table table = connection.getTable(tempTable);
             ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                assertThat(result.getValue(family, utf8ToBytes("dummy")), nullValue());
                results.add(bytesToStringMap(result.getFamilyMap(family)));
            }
        }

        assertThat(results.size(), equalTo(2));
        assertThat(results.get(0), equalTo(asStringMap("p-1", "a", "p-2", "b")));
        assertThat(results.get(1), equalTo(asStringMap("p-1", "one")));
    }

}
