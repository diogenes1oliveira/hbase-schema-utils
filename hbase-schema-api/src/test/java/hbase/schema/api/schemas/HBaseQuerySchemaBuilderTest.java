package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.testutils.DummyPojo;
import org.junit.jupiter.api.Test;

import static hbase.schema.api.converters.Utf8Converter.utf8Converter;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8ToBytes;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class HBaseQuerySchemaBuilderTest {
    @Test
    void build_SetsQualifiers() {
        HBaseQuerySchema<DummyPojo> schema = new HBaseQuerySchemaBuilder<DummyPojo>()
                .withRowKey(DummyPojo::getId, utf8Converter())
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
    void build_SetsPrefixes() {
        HBaseQuerySchema<DummyPojo> schema = new HBaseQuerySchemaBuilder<DummyPojo>()
                .withRowKey(DummyPojo::getId, utf8Converter())
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
