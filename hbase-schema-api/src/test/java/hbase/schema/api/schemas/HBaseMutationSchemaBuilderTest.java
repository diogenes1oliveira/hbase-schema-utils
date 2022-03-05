package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.api.interfaces.conversion.LongConverter;
import hbase.schema.api.testutils.DummyPojo;
import hbase.test.utils.models.PrettyBytesMap;
import hbase.test.utils.models.PrettyLongMap;
import org.junit.jupiter.api.Test;

import static hbase.schema.api.converters.Utf8BytesMapConverter.utf8BytesMapConverter;
import static hbase.schema.api.converters.Utf8Converter.utf8Converter;
import static hbase.schema.api.interfaces.conversion.LongMapConverter.longMapConverter;
import static hbase.schema.api.interfaces.conversion.LongMapConverter.longMapKeyConverter;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8ToBytes;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class HBaseMutationSchemaBuilderTest {
    @Test
    void withValues_SetsPrefixAccordingly() {
        HBaseMutationSchema<DummyPojo> schema = new HBaseMutationSchemaBuilder<DummyPojo>()
                .withTimestamp(DummyPojo::getLong)
                .withRowKey(DummyPojo::getId, utf8Converter())
                .withValues("prefix-", DummyPojo::getMap1, utf8BytesMapConverter())
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L).withMap1(singletonMap("key", "value"));

        assertThat(new PrettyBytesMap(schema.buildPutValues(pojo)), equalTo(new PrettyBytesMap(asBytesTreeMap(
                utf8ToBytes("prefix-key"), utf8ToBytes("value")
        ))));
    }

    @Test
    void withDeltas_SetsPrefixAccordingly() {
        HBaseMutationSchema<DummyPojo> schema = new HBaseMutationSchemaBuilder<DummyPojo>()
                .withTimestamp(DummyPojo::getLong)
                .withRowKey(DummyPojo::getId, utf8Converter())
                .withDeltas("prefix-", DummyPojo::getMap3, longMapKeyConverter(utf8Converter()))
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L).withMap3(singletonMap("key", 43L));

        assertThat(new PrettyLongMap(schema.buildIncrementValues(pojo)), equalTo(new PrettyLongMap(asBytesTreeMap(
                utf8ToBytes("prefix-key"), 43L
        ))));
    }

    @Test
    void withValue_SetsCellAccordingly() {
        HBaseMutationSchema<DummyPojo> schema = new HBaseMutationSchemaBuilder<DummyPojo>()
                .withTimestamp(DummyPojo::getLong)
                .withRowKey(DummyPojo::getId, utf8Converter())
                .withValue("some-string-field", DummyPojo::getField, utf8Converter())
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L).withField("some-value");

        assertThat(new PrettyBytesMap(schema.buildPutValues(pojo)), equalTo(new PrettyBytesMap(asBytesTreeMap(
                utf8ToBytes("some-string-field"), utf8ToBytes("some-value")
        ))));
    }

    @Test
    void withDelta_SetsCellAccordingly() {
        HBaseMutationSchema<DummyPojo> schema = new HBaseMutationSchemaBuilder<DummyPojo>()
                .withTimestamp(DummyPojo::getLong)
                .withRowKey(DummyPojo::getId, utf8Converter())
                .withDelta("some-delta-field", DummyPojo::getLong)
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L);

        assertThat(new PrettyLongMap(schema.buildIncrementValues(pojo)), equalTo(new PrettyLongMap(asBytesTreeMap(
                utf8ToBytes("some-delta-field"), 42L
        ))));
    }

    @Test
    void withTimestamp_setsFieldTimestamps() {
        HBaseMutationSchema<DummyPojo> schema = new HBaseMutationSchemaBuilder<DummyPojo>()
                .withTimestamp(DummyPojo::getLong)
                .withRowKey(DummyPojo::getId, utf8Converter())
                .withDeltas("first", DummyPojo::getMap3, longMapKeyConverter(utf8Converter()))
                .withTimestamp(obj -> obj.getLong() + 10L)
                .withDelta("second", DummyPojo::getLong)
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L).withMap3(singletonMap("1", 10L));

        assertThat(schema.buildTimestamp(pojo), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, null), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, utf8ToBytes("aaaa")), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, utf8ToBytes("zzzz")), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, utf8ToBytes("first-1")), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, utf8ToBytes("first-2")), equalTo(42L));
        assertThat(schema.buildTimestamp(pojo, utf8ToBytes("second")), equalTo(52L));
    }

}
