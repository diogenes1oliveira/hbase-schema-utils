package hbase.schema.connector;

import hbase.connector.HBaseConnector;
import hbase.schema.api.models.HBaseGenericRow;
import hbase.schema.api.schemas.AbstractHBasePojoWriteSchema;
import hbase.schema.api.schemas.HBaseGenericRowSchema;
import hbase.schema.api.schemas.HBasePojoWriteSchemaBuilder;
import hbase.test.utils.HBaseTestJunitExtension;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import testutils.DummyPojo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static hbase.schema.api.utils.HBaseConverters.utf8Converter;
import static hbase.test.utils.HBaseTestHelpers.createTable;
import static hbase.test.utils.HBaseTestHelpers.newTableDescriptor;

@ExtendWith(HBaseTestJunitExtension.class)
class AbstractHBasePojoWriteSchemaTest {

    AbstractHBasePojoWriteSchema<DummyPojo> pojoSchema = new HBasePojoWriteSchemaBuilder<>(DummyPojo::new)
            .withRowKey(DummyPojo::getId, utf8Converter())
            .withValueField("string-field", DummyPojo::getStringField, utf8Converter())
            .build();
    HBaseGenericRowSchema genericSchema = new HBaseGenericRowSchema();
    HBaseSchemaConnector<HBaseGenericRow> genericConnector;
    HBaseSchemaConnector<DummyPojo> dummyWriteConnector;
    byte[] family = new byte[]{'f'};
    TableName tempTable;

    @BeforeEach
    void setUp(TableName tempTable, HBaseConnector connector) {
        this.tempTable = tempTable;
        createTable(connector, newTableDescriptor(tempTable, family));

        genericConnector = new HBaseSchemaConnector<>(genericSchema, genericSchema, connector, tempTable, family);
        dummyWriteConnector = new HBaseSchemaConnector<>(null, pojoSchema, connector, tempTable, family);
    }

    @Test
    void writeAndReadBackWithGeneric() throws IOException {
        DummyPojo pojo = new DummyPojo();
        pojo.setId("some-id");

        dummyWriteConnector.mutate(pojo);
        int a = 1;
    }
}
