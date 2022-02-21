package hbase.schema.connector;

import hbase.connector.HBaseConnector;
import hbase.schema.api.models.HBaseGenericRow;
import hbase.schema.api.schemas.HBaseGenericRowSchema;
import hbase.test.utils.HBaseTestJunitExtension;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.time.Instant;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.test.utils.HBaseTestHelpers.createTable;
import static hbase.test.utils.HBaseTestHelpers.newTableDescriptor;
import static hbase.test.utils.HBaseTestHelpers.utf8ToBytes;
import static hbase.test.utils.models.PrettyEqualsBytesMap.prettifyBytesMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(HBaseTestJunitExtension.class)
class HBaseGenericRowSchemaSchemaIT {

    HBaseGenericRowSchema schema = new HBaseGenericRowSchema();
    HBaseSchemaConnector<HBaseGenericRow> schemaConnector;
    byte[] family = new byte[]{'f'};
    TableName tempTable;

    @BeforeEach
    void setUp(TableName tempTable, HBaseConnector connector) {
        this.tempTable = tempTable;
        createTable(connector, newTableDescriptor(tempTable, family));

        schemaConnector = new HBaseSchemaConnector<>(schema, schema, connector, tempTable, family);
    }

    @Test
    void writeWithGeneric(HBaseConnector connector) throws IOException {
        byte[] rowKey = utf8ToBytes("generic-row-key");
        byte[] qualifier = utf8ToBytes("some-column");
        byte[] value = utf8ToBytes("some-value");
        TreeMap<byte[], byte[]> cellsMap = asBytesTreeMap(qualifier, value);

        HBaseGenericRow row = new HBaseGenericRow(
                rowKey,
                Instant.now().toEpochMilli(),
                cellsMap
        );
        schemaConnector.mutate(row);

        Get get = new Get(rowKey).addFamily(family);
        Put put = new Put(rowKey).addColumn(family, utf8ToBytes("other-qualifier"), utf8ToBytes("other-value"));

        try (Connection connection = connector.connect();
             Table table = connection.getTable(tempTable)) {
            table.put(put);

            Result result = table.get(get);
            assertThat(result.getRow(), not(nullValue()));
            assertThat(result.getRow(), equalTo(rowKey));

            NavigableMap<byte[], byte[]> fetchedCellsMap = result.getFamilyMap(family);
            assertThat(fetchedCellsMap.get(qualifier), equalTo(value));
        }
    }

    @Test
    void readWithGeneric(HBaseConnector connector) throws IOException {
        byte[] rowKey = utf8ToBytes("generic-row-key");
        byte[] qualifier = utf8ToBytes("some-column");
        byte[] value = utf8ToBytes("some-value");
        Put put = new Put(rowKey)
                .addColumn(family, qualifier, value);

        try (Connection connection = connector.connect();
             Table table = connection.getTable(tempTable)) {
            table.put(put);
        }

        HBaseGenericRow query = new HBaseGenericRow(rowKey, asBytesTreeMap());
        HBaseGenericRow result = schemaConnector.get(query).orElse(null);

        assertThat(result, not(nullValue()));
        SortedMap<byte[], byte[]> expected = asBytesTreeMap(qualifier, value);
        SortedMap<byte[], byte[]> actual = result.getBytesCells();

        assertThat(prettifyBytesMap(actual), equalTo(prettifyBytesMap(expected)));
    }

}
