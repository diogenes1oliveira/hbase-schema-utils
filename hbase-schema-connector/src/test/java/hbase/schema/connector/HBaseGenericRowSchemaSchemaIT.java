package hbase.schema.connector;

import hbase.test.utils.HBaseTestJunitExtension;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HBaseTestJunitExtension.class)
class HBaseGenericRowSchemaSchemaIT {
//
//    HBaseGenericRowSchemaOld schema = new HBaseGenericRowSchemaOld();
//    HBaseSchemaConnector<HBaseGenericRow> schemaConnector;
//    byte[] family = new byte[]{'f'};
//    TableName tempTable;
//
//    @BeforeEach
//    void setUp(TableName tempTable, HBaseConnector connector) {
//        this.tempTable = tempTable;
//        createTable(connector, newTableDescriptor(tempTable, family));
//
//        schemaConnector = new HBaseSchemaConnector<>(schema, schema, connector, tempTable, family);
//    }
//
//    @Test
//    void writeWithGeneric(HBaseConnector connector) throws IOException {
//        byte[] rowKey = utf8ToBytes("generic-row-key");
//        byte[] qualifier = utf8ToBytes("some-column");
//        byte[] value = utf8ToBytes("some-value");
//        TreeMap<byte[], byte[]> cellsMap = asBytesTreeMap(qualifier, value);
//
//        HBaseGenericRow row = new HBaseGenericRow(
//                rowKey,
//                Instant.now().toEpochMilli(),
//                cellsMap
//        );
//        schemaConnector.mutate(row);
//
//        Get get = new Get(rowKey).addFamily(family);
//        Put put = new Put(rowKey).addColumn(family, utf8ToBytes("other-qualifier"), utf8ToBytes("other-value"));
//
//        try (Connection connection = connector.connect();
//             Table table = connection.getTable(tempTable)) {
//            table.put(put);
//
//            Result result = table.get(get);
//            assertThat(result.getRow(), not(nullValue()));
//            assertThat(result.getRow(), equalTo(rowKey));
//
//            NavigableMap<byte[], byte[]> fetchedCellsMap = result.getFamilyMap(family);
//            assertThat(fetchedCellsMap.get(qualifier), equalTo(value));
//        }
//    }
//
//    @Test
//    void readWithGeneric(HBaseConnector connector) throws IOException {
//        byte[] rowKey = utf8ToBytes("generic-row-key");
//        byte[] qualifier = utf8ToBytes("some-column");
//        byte[] value = utf8ToBytes("some-value");
//        Put put = new Put(rowKey)
//                .addColumn(family, qualifier, value);
//
//        try (Connection connection = connector.connect();
//             Table table = connection.getTable(tempTable)) {
//            table.put(put);
//        }
//
//        HBaseGenericRow query = new HBaseGenericRow(rowKey, asBytesTreeMap());
//        HBaseGenericRow result = schemaConnector.get(query).orElse(null);
//
//        assertThat(result, not(nullValue()));
//        SortedMap<byte[], byte[]> expected = asBytesTreeMap(qualifier, value);
//        SortedMap<byte[], byte[]> actual = result.getBytesCells();
//
//        assertThat(prettifyBytesMap(actual), equalTo(prettifyBytesMap(expected)));
//    }

}
