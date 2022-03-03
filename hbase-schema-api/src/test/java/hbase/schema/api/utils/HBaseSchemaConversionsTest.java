package hbase.schema.api.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import hbase.schema.api.interfaces.converters.HBaseBytesGetter;
import hbase.schema.api.interfaces.converters.HBaseBytesMapGetter;
import hbase.schema.api.interfaces.converters.HBaseBytesMapSetter;
import hbase.schema.api.interfaces.converters.HBaseBytesSetter;
import hbase.schema.api.interfaces.converters.HBaseLongGetter;
import hbase.schema.api.testutils.DummyPojo;
import hbase.test.utils.HBaseTestHelpers;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.NavigableMap;

import static hbase.schema.api.interfaces.converters.HBaseLongGetter.longGetter;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8ToBytes;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

class HBaseSchemaConversionsTest {

    @Test
    void stringGetter_ConvertsString() {
        Instant instant = Instant.parse("2021-01-01T01:02:03Z");

        HBaseBytesGetter<DummyPojo> getter = HBaseSchemaConversions.stringGetter(DummyPojo::getInstant, Instant::toString);
        HBaseBytesSetter<DummyPojo> setter = HBaseSchemaConversions.stringSetter(DummyPojo::setInstant, Instant::parse);

        DummyPojo input = new DummyPojo().withInstant(instant);
        assertThat(getter.getBytes(input), equalTo("2021-01-01T01:02:03Z".getBytes(StandardCharsets.UTF_8)));

        DummyPojo output = new DummyPojo();
        setter.setFromBytes(output, getter.getBytes(input));
        assertThat(output.getInstant(), equalTo(instant));
    }

    @Test
    void stringGetter_HandlesNull() {
        HBaseBytesGetter<DummyPojo> getter = HBaseSchemaConversions.stringGetter(DummyPojo::getInstant, Instant::toString);

        DummyPojo input = new DummyPojo();
        assertThat(getter.getBytes(input), nullValue());
    }

    @Test
    void longGetter_ConvertsLong() {
        Instant instant = Instant.parse("1970-01-01T00:00:42Z");

        HBaseLongGetter<DummyPojo> getter = longGetter(DummyPojo::getInstant, Instant::toEpochMilli);
        HBaseBytesSetter<DummyPojo> setter = HBaseSchemaConversions.longSetter(DummyPojo::setInstant, Instant::ofEpochMilli);

        DummyPojo input = new DummyPojo().withInstant(instant);
        assertThat(getter.getLong(input), equalTo(42_000L));

        DummyPojo output = new DummyPojo();
        setter.setFromBytes(output, getter.getBytes(input));
        assertThat(output.getInstant(), equalTo(instant));
    }

    @Test
    void booleanGetter_ConvertsBoolean() {
        HBaseLongGetter<DummyPojo> getter = HBaseSchemaConversions.booleanGetter(DummyPojo::getField, s -> {
            switch (s) {
                case "null":
                    return null;
                case "true":
                    return true;
                case "false":
                    return false;
                default:
                    throw new IllegalStateException();
            }
        });

        DummyPojo nullPojo = new DummyPojo();
        assertThat(getter.getLong(nullPojo), nullValue());

        DummyPojo falsePojo = new DummyPojo().withField("false");
        assertThat(getter.getLong(falsePojo), nullValue());

        DummyPojo truePojo = new DummyPojo().withField("true");
        assertThat(getter.getLong(truePojo), equalTo(1L));
    }

    @SuppressWarnings("unchecked")
    @Test
    void jsonGetter_ConvertsJson() throws IOException {
        ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        HBaseBytesGetter<DummyPojo> getter = HBaseSchemaConversions.jsonGetter(mapper);
        HBaseBytesSetter<DummyPojo> setter = HBaseSchemaConversions.jsonSetter(mapper);

        DummyPojo input = new DummyPojo().withId("my id");
        byte[] inputJson = getter.getBytes(input);
        Map<String, String> inputMap = mapper.readValue(inputJson, Map.class);
        assertThat(inputMap, equalTo(singletonMap("id", "my id")));

        DummyPojo output = new DummyPojo();
        setter.setFromBytes(output, inputJson);
        assertThat(output, equalTo(input));
    }

    @Test
    void listColumnGetter_DoesYieldListOfCells() {
        HBaseBytesMapGetter<DummyPojo> getter = HBaseSchemaConversions.listColumnGetter(
                DummyPojo::getListField,
                s -> utf8ToBytes("item-" + s),
                s -> utf8ToBytes("value-" + s)
        );
        DummyPojo input = new DummyPojo().withListField(asList("1", "2"));

        NavigableMap<byte[], byte[]> bytesMap = getter.getBytesMap(input);

        assertThat(bytesMap.size(), equalTo(2));
        assertThat(bytesMap.get(utf8ToBytes("item-1")), equalTo(utf8ToBytes("value-1")));
        assertThat(bytesMap.get(utf8ToBytes("item-2")), equalTo(utf8ToBytes("value-2")));
    }

    @Test
    void listColumnGetter_HandlesNulls() {
        HBaseBytesMapGetter<DummyPojo> getter = HBaseSchemaConversions.listColumnGetter(
                DummyPojo::getListField,
                s -> StringUtils.isEmpty(s) ? null : utf8ToBytes(s),
                HBaseTestHelpers::asUtf8
        );
        DummyPojo pojoNullList = new DummyPojo();
        DummyPojo pojoEmptyList = new DummyPojo().withListField(emptyList());
        DummyPojo pojoNullValues = new DummyPojo().withListField(asList(null, ""));

        assertThat(getter.getBytesMap(pojoNullList).keySet(), empty());
        assertThat(getter.getBytesMap(pojoEmptyList).keySet(), empty());
        assertThat(getter.getBytesMap(pojoNullValues).keySet(), empty());
    }

    @Test
    void listColumnSetter_DoesBuildListOfCells() {
        HBaseBytesMapSetter<DummyPojo> setter = HBaseSchemaConversions.listColumnSetter(
                DummyPojo::setListField,
                HBaseSchemaConversions::utf8FromBytes
        );
        DummyPojo input = new DummyPojo();

        setter.setFromBytes(input, asBytesTreeMap(
                utf8ToBytes("k1"), utf8ToBytes("v1"),
                utf8ToBytes("k2"), utf8ToBytes("v2")
        ));

        assertThat(input.getListField(), equalTo(asList("v1", "v2")));
    }

}
