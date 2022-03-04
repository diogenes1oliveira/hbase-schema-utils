package hbase.schema.connector.services;

import hbase.connector.services.HBaseConnector;
import hbase.schema.api.interfaces.HBaseResultParserSchema;
import hbase.schema.api.schemas.HBaseResultParserSchemaBuilder;
import hbase.test.utils.HBaseTestJunitExtension;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import testutils.DummyPojo;

import java.io.IOException;
import java.util.Properties;

import static hbase.schema.api.utils.HBaseSchemaConversions.stringSetter;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8ToBytes;
import static hbase.schema.api.utils.HBaseSchemaUtils.asStringMap;
import static hbase.test.utils.HBaseTestHelpers.createTable;
import static hbase.test.utils.HBaseTestHelpers.newTableDescriptor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static testutils.HBaseSchemaConnectorTestHelpers.bytesToStringMap;

@ExtendWith(HBaseTestJunitExtension.class)
class HBaseSchemaResultsParserIT {
    byte[] family = new byte[]{'f'};
    TableName tempTable;
    HBaseConnector connector;

    HBaseResultParserSchema<DummyPojo> resultParserSchema = new HBaseResultParserSchemaBuilder<>(DummyPojo::new)
            .fromRowKey(stringSetter(DummyPojo::setId))
            .fromPrefix("p-", (obj, bytesMap) -> obj.setMap1(bytesToStringMap(bytesMap)))
            .fromColumn("string", stringSetter(DummyPojo::setString))
            .build();
    HBaseSchemaResultsParser<DummyPojo> resultsParser = new HBaseSchemaResultsParser<>(family, resultParserSchema);

    @BeforeEach
    void setUp(TableName tempTable, Properties props, Connection connection) {
        this.tempTable = tempTable;
        this.connector = new HBaseConnector(props);

        createTable(connection, newTableDescriptor(tempTable, family));
    }

    @Test
    void testParsing() throws IOException {
        Put put = new Put(utf8ToBytes("some-id"))
                .addColumn(family, utf8ToBytes("p-1"), utf8ToBytes("a"))
                .addColumn(family, utf8ToBytes("p-2"), utf8ToBytes("b"))
                .addColumn(family, utf8ToBytes("string"), utf8ToBytes("some-value"));
        Get get = new Get(utf8ToBytes("some-id")).addFamily(family);
        DummyPojo result;

        try (Connection connection = connector.context();
             Table table = connection.getTable(tempTable)) {
            table.put(put);
            result = resultsParser.parseResult(table.get(get));
        }

        assertThat(result, notNullValue());
        assertThat(result.getId(), equalTo("some-id"));
        assertThat(result.getString(), equalTo("some-value"));
        assertThat(result.getMap1(), equalTo(asStringMap("1", "a", "2", "b")));
    }
}
