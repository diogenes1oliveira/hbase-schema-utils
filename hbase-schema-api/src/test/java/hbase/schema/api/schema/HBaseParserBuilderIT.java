package hbase.schema.api.schema;

import hbase.schema.api.interfaces.HBaseParser;
import hbase.schema.api.testutils.HBaseDml;
import hbase.test.utils.HBaseTestJunitExtension;
import hbase.test.utils.interfaces.HBaseTestInstance;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static hbase.test.utils.HBaseTestHelpers.createTable;
import static hbase.test.utils.HBaseTestHelpers.newTableDescriptor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ExtendWith(HBaseTestJunitExtension.class)
class HBaseParserBuilderIT {

    static String family = "f";
    static String tableName;
    HBaseDml dml;

    @BeforeAll
    static void setUpTable(TableName tempTable, HBaseTestInstance testInstance, Connection connection) {
        testInstance.cleanUp();
        tableName = tempTable.getNameAsString();
        createTable(connection, newTableDescriptor(tableName, family));
    }

    @BeforeEach
    void setUp(Connection connection) {
        dml = new HBaseDml(connection, tableName, family);
    }

    @Test
    void parsesRowKey() {
        HBaseParser parser = new HBaseParserBuilder(family)
                .separator("|")
                .fragment("user")
                .constant("v01")
                .fragment("session")
                .column("dummy")
                .build();

        dml.put("someone|v01|somewhere", "dummy", "0");
        Result hBaseResult = dml.get("someone|v01|somewhere");
        Map<String, Object> result = parser.parse(hBaseResult);

        assertThat(result.get("user"), equalTo("someone"));
        assertThat(result.get("session"), equalTo("somewhere"));
    }

    @Test
    void parsesColumnData() {
        HBaseParser parser = new HBaseParserBuilder(family)
                .column("string_field")
                .column("long_field", ByteBuffer::getLong)
                .build();

        dml.put("row", "string_field", "value");
        dml.increment("row", "long_field", 21L);
        dml.increment("row", "long_field", 21L);
        Result hBaseResult = dml.get("row");
        Map<String, Object> result = parser.parse(hBaseResult);

        assertThat(result.get("string_field"), equalTo("value"));
        assertThat(result.get("long_field"), equalTo(42L));
    }

    @Test
    void parsesColumnPrefixes() {
        HBaseParser parser = new HBaseParserBuilder(family)
                .prefix("p:")
                .build();

        dml.put("row", "p", "v1", "p:1", "a", "p:2", "b");
        Result hBaseResult = dml.get("row");
        Map<String, Object> result = parser.parse(hBaseResult);

        assertThat(result.get("p:"), equalTo(new HashMap<String, String>() {{
            put("1", "a");
            put("2", "b");
        }}));
    }

}
