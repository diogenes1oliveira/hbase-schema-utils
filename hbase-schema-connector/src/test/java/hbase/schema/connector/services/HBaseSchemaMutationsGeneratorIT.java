package hbase.schema.connector.services;

import hbase.connector.HBaseConnector;
import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.api.schemas.HBaseMutationSchemaBuilder;
import hbase.test.utils.HBaseTestJunitExtension;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import testutils.DummyPojo;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static hbase.schema.api.interfaces.converters.HBaseLongGetter.longGetter;
import static hbase.schema.api.utils.HBaseSchemaConversions.stringGetter;
import static hbase.schema.api.utils.HBaseSchemaUtils.asStringMap;
import static hbase.test.utils.HBaseTestHelpers.createTable;
import static hbase.test.utils.HBaseTestHelpers.newTableDescriptor;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static testutils.HBaseSchemaConnectorTestHelpers.bytesToLongMap;
import static testutils.HBaseSchemaConnectorTestHelpers.bytesToStringMap;

@ExtendWith(HBaseTestJunitExtension.class)
class HBaseSchemaMutationsGeneratorIT {
    TableName tempTable;
    byte[] family = new byte[]{'f'};
    HBaseMutationSchema<DummyPojo> schema = new HBaseMutationSchemaBuilder<DummyPojo>()
            .withTimestamp(longGetter(DummyPojo::getInstant, Instant::toEpochMilli))
            .withRowKey(stringGetter(DummyPojo::getId))
            .withValue("string", stringGetter(DummyPojo::getString))
            .withTimestamp(longGetter(DummyPojo::getInstant, instant -> instant.toEpochMilli() + 42_000L))
            .withDelta("long", DummyPojo::getLong)
            .build();
    HBaseSchemaMutationsGenerator<DummyPojo> mutationsGenerator = new HBaseSchemaMutationsGenerator<>(family, schema);

    @BeforeEach
    void setUp(TableName tempTable, HBaseConnector connector) {
        this.tempTable = tempTable;
        createTable(connector, newTableDescriptor(tempTable, family));
    }

    @Test
    void testValues(HBaseConnector connector) throws IOException, InterruptedException {
        DummyPojo pojo = new DummyPojo()
                .withId("dummy-id")
                .withInstant(Instant.ofEpochMilli(1000L))
                .withString("dummy-string");
        List<Mutation> mutations = mutationsGenerator.toMutations(pojo);

        try (Connection connection = connector.connect();
             Table table = connection.getTable(tempTable)) {
            table.batch(mutations, new Object[mutations.size()]);
        }

        Scan scan = new Scan().addFamily(family);
        List<TreeMap<String, String>> results = new ArrayList<>();

        try (Connection connection = connector.connect();
             Table table = connection.getTable(tempTable);
             ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                results.add(bytesToStringMap(result.getFamilyMap(family)));
            }
        }

        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0), equalTo(asStringMap("string", "dummy-string")));
    }

    @Test
    void testDeltas(HBaseConnector connector) throws IOException, InterruptedException {
        DummyPojo pojo1 = new DummyPojo()
                .withId("dummy-id")
                .withInstant(Instant.ofEpochMilli(1000L))
                .withLong(2L);
        DummyPojo pojo2 = new DummyPojo()
                .withId("dummy-id")
                .withInstant(Instant.ofEpochMilli(1001L))
                .withLong(3L);

        try (Connection connection = connector.connect();
             Table table = connection.getTable(tempTable)) {
            table.batch(mutationsGenerator.toMutations(pojo1), new Object[1]);
            table.batch(mutationsGenerator.toMutations(pojo2), new Object[1]);
        }

        Scan scan = new Scan().addFamily(family);
        List<TreeMap<String, Long>> results = new ArrayList<>();

        try (Connection connection = connector.connect();
             Table table = connection.getTable(tempTable);
             ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                results.add(bytesToLongMap(result.getFamilyMap(family)));
            }
        }

        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0), equalTo(singletonMap("long", 5L)));
    }
}
