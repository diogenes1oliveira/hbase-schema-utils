package hbase.schema.api.utils;

import hbase.schema.api.interfaces.converters.HBaseBytesGetter;
import hbase.schema.api.interfaces.converters.HBaseBytesSetter;
import hbase.schema.api.interfaces.converters.HBaseLongGetter;
import hbase.schema.api.testutils.DummyPojo;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static hbase.schema.api.interfaces.converters.HBaseLongGetter.longGetter;
import static org.hamcrest.MatcherAssert.assertThat;
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
}
