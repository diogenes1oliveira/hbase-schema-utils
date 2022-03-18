package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseMutationMapper;
import hbase.schema.api.models.HBaseDeltaCell;
import hbase.schema.api.models.HBaseValueCell;
import hbase.schema.api.testutils.DummyPojo;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static hbase.schema.api.converters.InstantLongConverter.instantLongConverter;
import static hbase.schema.api.converters.Utf8BytesMapConverter.utf8BytesMapConverter;
import static hbase.schema.api.converters.Utf8Converter.utf8Converter;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8ToBytes;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class HBaseMutationMapperBuilderTest {
    @Test
    void prefix_SetsPrefixAccordingly() {
        HBaseMutationMapper<DummyPojo> schema = new HBaseMutationMapperBuilder<DummyPojo>()
                .timestampLong(DummyPojo::getLong)
                .rowKey(DummyPojo::getId, utf8Converter())
                .prefix("prefix-", DummyPojo::getMap1, utf8BytesMapConverter())
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L).withMap1(singletonMap("key", "value"));

        assertThat(schema.toRowKey(pojo), equalTo(utf8ToBytes("some-id")));
        assertThat(schema.toTimestamp(pojo), equalTo(42L));
        assertThat(schema.toValues(pojo), equalTo(singletonList(
                new HBaseValueCell(utf8ToBytes("prefix-key"), utf8ToBytes("value"), 42L))
        ));
    }

    @Test
    void column_SetsCellAccordingly() {
        HBaseMutationMapper<DummyPojo> schema = new HBaseMutationMapperBuilder<DummyPojo>()
                .timestamp(DummyPojo::getInstant, instantLongConverter())
                .rowKey(DummyPojo::getId, utf8Converter())
                .column("some-string-field", DummyPojo::getString, utf8Converter())
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withInstant(Instant.ofEpochMilli(42L)).withString("some-value");

        assertThat(schema.toValues(pojo), equalTo(singletonList(
                new HBaseValueCell(utf8ToBytes("some-string-field"), utf8ToBytes("some-value"), 42L))
        ));
    }

    @Test
    void withDelta_SetsCellAccordingly() {
        HBaseMutationMapper<DummyPojo> schema = new HBaseMutationMapperBuilder<DummyPojo>()
                .timestampLong(DummyPojo::getLong)
                .rowKey(DummyPojo::getId, utf8Converter())
                .deltaLong("some-delta-field", DummyPojo::getLong)
                .build();

        DummyPojo pojo = new DummyPojo().withId("some-id").withLong(42L);

        assertThat(schema.toValues(pojo), equalTo(singletonList(
                new HBaseDeltaCell(utf8ToBytes("some-string-field"), 42L))
        ));
    }

    @Test
    void withTimestamp_setsFieldTimestamps() {
        HBaseMutationMapper<DummyPojo> schema = new HBaseMutationMapperBuilder<DummyPojo>()
                .timestampLong(DummyPojo::getLong)
                .rowKey(DummyPojo::getId, utf8Converter())
                .prefix("first:", DummyPojo::getMap1, utf8BytesMapConverter())
                .timestampLong(obj -> obj.getLong() + 10L)
                .column("last", DummyPojo::getString, utf8Converter())
                .build();

        Map<String, String> map = new HashMap<String, String>() {{
            put("a", "1");
            put("b", "2");
        }};
        DummyPojo pojo = new DummyPojo()
                .withId("some-id")
                .withLong(42L)
                .withMap1(map);

        List<HBaseValueCell> cells = schema.toValues(pojo);
        Collections.sort(cells);

        assertThat(cells.get(0).getQualifier(), equalTo(utf8ToBytes("first:a")));
        assertThat(cells.get(0).getTimestamp(), equalTo(42L));

        assertThat(cells.get(1).getQualifier(), equalTo(utf8ToBytes("first:b")));
        assertThat(cells.get(1).getTimestamp(), equalTo(42L));

        assertThat(cells.get(2).getQualifier(), equalTo(utf8ToBytes("last")));
        assertThat(cells.get(2).getTimestamp(), equalTo(52L));
    }

}
