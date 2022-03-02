package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.api.models.PrettyBytesMap;
import hbase.schema.api.models.PrettyLongMap;
import hbase.schema.api.testutils.DummyPojo;
import org.junit.jupiter.api.Test;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.stringGetter;
import static hbase.schema.api.utils.HBaseSchemaUtils.utf8ToBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class HBaseMutationSchemaBuilderTest {
    @Test
    void withValues_SetsPrefixAccordingly() {
        HBaseMutationSchema<DummyPojo> schema = new HBaseMutationSchemaBuilder<DummyPojo>()
                .withTimestamp(DummyPojo::getLong)
                .withRowKey(stringGetter(DummyPojo::getId))
                .withValues("prefix-", obj -> asBytesTreeMap(
                        utf8ToBytes("key"), utf8ToBytes("value")
                ))
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L);

        assertThat(new PrettyBytesMap(schema.buildCellValues(pojo)), equalTo(new PrettyBytesMap(asBytesTreeMap(
                utf8ToBytes("prefix-key"), utf8ToBytes("value")
        ))));
    }

    @Test
    void withDeltas_SetsPrefixAccordingly() {
        HBaseMutationSchema<DummyPojo> schema = new HBaseMutationSchemaBuilder<DummyPojo>()
                .withTimestamp(DummyPojo::getLong)
                .withRowKey(stringGetter(DummyPojo::getId))
                .withDeltas("prefix-", obj -> asBytesTreeMap(
                        utf8ToBytes("key"), 43L
                ))
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L);

        assertThat(new PrettyLongMap(schema.buildCellIncrements(pojo)), equalTo(new PrettyLongMap(asBytesTreeMap(
                utf8ToBytes("prefix-key"), 43L
        ))));
    }

    @Test
    void withValue_SetsCellAccordingly() {
        HBaseMutationSchema<DummyPojo> schema = new HBaseMutationSchemaBuilder<DummyPojo>()
                .withTimestamp(DummyPojo::getLong)
                .withRowKey(stringGetter(DummyPojo::getId))
                .withValue("some-string-field", stringGetter(DummyPojo::getField))
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L).withField("some-value");

        assertThat(new PrettyBytesMap(schema.buildCellValues(pojo)), equalTo(new PrettyBytesMap(asBytesTreeMap(
                utf8ToBytes("some-string-field"), utf8ToBytes("some-value")
        ))));
    }

    @Test
    void withDelta_SetsCellAccordingly() {
        HBaseMutationSchema<DummyPojo> schema = new HBaseMutationSchemaBuilder<DummyPojo>()
                .withTimestamp(DummyPojo::getLong)
                .withRowKey(stringGetter(DummyPojo::getId))
                .withDelta("some-delta-field", DummyPojo::getLong)
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L);

        assertThat(new PrettyLongMap(schema.buildCellIncrements(pojo)), equalTo(new PrettyLongMap(asBytesTreeMap(
                utf8ToBytes("some-delta-field"), 42L
        ))));
    }

    @Test
    void withTimestamp_setsFieldTimestamps() {
        HBaseMutationSchema<DummyPojo> schema = new HBaseMutationSchemaBuilder<DummyPojo>()
                .withTimestamp(DummyPojo::getLong)
                .withRowKey(stringGetter(DummyPojo::getId))
                .withDeltas("first", obj -> asBytesTreeMap(utf8ToBytes("1"), 10L))
                .withTimestamp(obj -> obj.getLong() + 10L)
                .withDelta("second", DummyPojo::getLong)
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L);

        assertThat(schema.buildTimestamp(pojo), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, null), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, utf8ToBytes("aaaa")), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, utf8ToBytes("zzzz")), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, utf8ToBytes("first-1")), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, utf8ToBytes("first-2")), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, utf8ToBytes("second")), equalTo(52L));
    }

}
