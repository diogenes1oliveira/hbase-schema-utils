package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.testutils.DummyPojo;
import org.junit.jupiter.api.Test;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static hbase.schema.api.utils.HBaseConversions.stringGetter;
import static hbase.test.utils.HBaseTestHelpers.utf8ToBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class HBaseQuerySchemaBuilderTest {
    @Test
    void buildScanKey_SlicesRowKey() {
        HBaseQuerySchema<DummyPojo> schema = new HBaseQuerySchemaBuilder<DummyPojo>()
                .withRowKey(stringGetter(DummyPojo::getId))
                .withScanKeySize(2)
                .withQualifiers("dummy")
                .build();

        DummyPojo query = new DummyPojo();
        query.setId("ab-123");
        assertThat(schema.buildScanKey(query), equalTo(new byte[]{'a', 'b'}));
    }

    @Test
    void buildScanKey_UsesRowKeyByDefault() {
        HBaseQuerySchema<DummyPojo> schema = new HBaseQuerySchemaBuilder<DummyPojo>()
                .withRowKey(stringGetter(DummyPojo::getId))
                .withQualifiers("dummy")
                .build();

        DummyPojo query = new DummyPojo();
        query.setId("ab-123");

        assertThat(schema.buildRowKey(query), equalTo(utf8ToBytes("ab-123")));
        assertThat(schema.buildScanKey(query), equalTo(utf8ToBytes("ab-123")));
    }

    @Test
    void buildScanKey_SetsQualifiers() {
        HBaseQuerySchema<DummyPojo> schema = new HBaseQuerySchemaBuilder<DummyPojo>()
                .withRowKey(stringGetter(DummyPojo::getId))
                .withQualifiers("q1", "q2", "q3")
                .build();

        DummyPojo query = new DummyPojo();
        query.setId("ab-123");

        assertThat(schema.getQualifiers(query), equalTo(asBytesTreeSet(
                utf8ToBytes("q1"),
                utf8ToBytes("q2"),
                utf8ToBytes("q3")
        )));
        assertThat(schema.getPrefixes(query), equalTo(asBytesTreeSet()));
    }

    @Test
    void buildScanKey_SetsPrefixes() {
        HBaseQuerySchema<DummyPojo> schema = new HBaseQuerySchemaBuilder<DummyPojo>()
                .withRowKey(stringGetter(DummyPojo::getId))
                .withPrefixes("q1", "q2", "q3")
                .build();

        DummyPojo query = new DummyPojo();
        query.setId("ab-123");

        assertThat(schema.getQualifiers(query), equalTo(asBytesTreeSet()));
        assertThat(schema.getPrefixes(query), equalTo(asBytesTreeSet(
                utf8ToBytes("q1"),
                utf8ToBytes("q2"),
                utf8ToBytes("q3")
        )));
    }
}
