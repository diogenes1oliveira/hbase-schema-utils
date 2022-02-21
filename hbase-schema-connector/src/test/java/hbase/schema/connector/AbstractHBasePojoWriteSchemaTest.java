package hbase.schema.connector;

import hbase.connector.HBaseConnector;
import hbase.schema.api.schemas.AbstractHBasePojoWriteSchema;
import hbase.schema.api.schemas.HBaseGenericRowSchema;
import hbase.test.utils.HBaseTestJunitExtension;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import testutils.DummyPojo;

import static hbase.schema.api.utils.HBaseConverters.utf8Converter;
import static hbase.test.utils.HBaseTestHelpers.createTable;
import static hbase.test.utils.HBaseTestHelpers.newTableDescriptor;

@ExtendWith(HBaseTestJunitExtension.class)
class AbstractHBasePojoWriteSchemaTest {

    AbstractHBasePojoWriteSchema<DummyPojo> pojoSchema = AbstractHBasePojoWriteSchema
            .newBuilder(DummyPojo.class)
            .withRowKey(DummyPojo::getId, utf8Converter())
            .build();

    HBaseGenericRowSchema genericSchema = new HBaseGenericRowSchema();
    TableName tempTable;
    HBaseConnector connector;
    String family = "cf";

    @BeforeEach
    void setUp(TableName tempTable, HBaseConnector connector) {
        this.tempTable = tempTable;
        createTable(connector, newTableDescriptor(tempTable, family));
    }
}
