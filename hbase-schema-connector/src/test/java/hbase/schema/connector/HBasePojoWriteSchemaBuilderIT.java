package hbase.schema.connector;

import hbase.test.utils.HBaseTestJunitExtension;
import org.junit.jupiter.api.extension.ExtendWith;

@SuppressWarnings("ConstantConditions")
@ExtendWith(HBaseTestJunitExtension.class)
class HBasePojoWriteSchemaBuilderIT {
//
//    String stringColumn = "string-field";
//    String counterColumn = "long-counter";
//    AbstractHBasePojoWriteSchema<DummyPojo> pojoSchema = new HBasePojoWriteSchemaBuilder<>(DummyPojo::new)
//            .withRowKey(DummyPojo::getId, utf8Converter())
//            .withValueField(stringColumn, DummyPojo::getStringField, utf8Converter())
//            .withDeltaField(counterColumn, DummyPojo::getCounterField)
//            .build();
//    HBaseGenericRowSchemaOld genericSchema = new HBaseGenericRowSchemaOld();
//    HBaseSchemaConnector<HBaseGenericRow> genericConnector;
//    HBaseSchemaConnector<DummyPojo> dummyWriteConnector;
//    byte[] family = new byte[]{'f'};
//    TableName tempTable;
//
//    @BeforeEach
//    void setUp(TableName tempTable, HBaseConnector connector) {
//        this.tempTable = tempTable;
//        createTable(connector, newTableDescriptor(tempTable, family));
//
//        genericConnector = new HBaseSchemaConnector<>(genericSchema, genericSchema, connector, tempTable, family);
//        dummyWriteConnector = new HBaseSchemaConnector<>(null, pojoSchema, connector, tempTable, family);
//    }
//
//    @Test
//    void writeAndReadBackWithGeneric() throws IOException {
//        String id = "row1";
//        String value1 = "value2";
//        String value2 = "value2";
//
//        DummyPojo pojo1 = new DummyPojo();
//        pojo1.setId(id);
//        pojo1.setStringField(value1);
//        pojo1.setCounterField(2);
//
//        DummyPojo pojo2 = new DummyPojo();
//        pojo2.setId(id);
//        pojo2.setStringField(value2);
//        pojo2.setCounterField(3);
//
//        dummyWriteConnector.mutate(pojo1);
//        safeSleep(100);
//        dummyWriteConnector.mutate(pojo2);
//
//        HBaseGenericRow query = new HBaseGenericRow(
//                utf8ToBytes(id),
//                null,
//                asBytesTreeMap(utf8ToBytes("string-field"), null),
//                asBytesTreeMap(utf8ToBytes("long-counter"), null)
//        );
//        List<HBaseGenericRow> results = genericConnector.get(singletonList(query));
//
//        assertThat(results.size(), equalTo(1));
//        HBaseGenericRow row = results.get(0);
//
//        assertThat(row.getRowKey(), equalTo(utf8ToBytes(id)));
//        assertThat(row.getLongCells().size(), equalTo(0));
//        assertThat(row.getBytesCells().size(), equalTo(2));
//
//        byte[] stringBytes = row.getBytesCells().get(utf8ToBytes(stringColumn));
//        byte[] counterBytes = row.getBytesCells().get(utf8ToBytes(counterColumn));
//
//        String stringValue = new String(stringBytes, StandardCharsets.UTF_8);
//        long counterValue = Bytes.toLong(counterBytes);
//
//        assertThat(stringValue, equalTo(value2));
//        assertThat(counterValue, equalTo(5L));
//    }

}
