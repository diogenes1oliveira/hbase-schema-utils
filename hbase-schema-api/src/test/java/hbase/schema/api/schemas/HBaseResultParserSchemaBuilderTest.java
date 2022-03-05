package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseResultParserSchema;
import hbase.schema.api.testutils.DummyPojo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.NavigableMap;
import java.util.stream.Stream;

import static hbase.schema.api.converters.Utf8BytesMapConverter.utf8BytesMapConverter;
import static hbase.schema.api.converters.Utf8Converter.utf8Converter;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8ToBytes;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.asStringMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class HBaseResultParserSchemaBuilderTest {

    HBaseResultParserSchema<DummyPojo> resultParser = new HBaseResultParserSchemaBuilder<>(DummyPojo::new)
            .fromRowKey(DummyPojo::setId, utf8Converter())
            .fromColumn("field", DummyPojo::setField, utf8Converter())
            .fromPrefix("p", DummyPojo::setMap1, utf8BytesMapConverter())
            .fromPrefix("q", DummyPojo::setMap2, utf8BytesMapConverter())
            .build();

    @ParameterizedTest
    @MethodSource("provideCases")
    void doesParseAccordingly(byte[] rowKey, NavigableMap<byte[], byte[]> cellsMap, DummyPojo expected) {
        DummyPojo actual = resultParser.newInstance();
        resultParser.setFromResult(actual, rowKey, cellsMap);

        assertThat(actual, equalTo(expected));
    }

    static Stream<Arguments> provideCases() {
        return Stream.of(
                Arguments.of(
                        utf8ToBytes("my row key"),
                        asBytesTreeMap(
                                utf8ToBytes("field"), utf8ToBytes("value"),
                                utf8ToBytes("fieldOther"), utf8ToBytes("other value"),
                                utf8ToBytes("p1"), utf8ToBytes("v1"),
                                utf8ToBytes("p2"), utf8ToBytes("v2"),
                                utf8ToBytes("q"), utf8ToBytes("v")
                        ),
                        new DummyPojo()
                                .withId("my row key")
                                .withField("value")
                                .withMap1(asStringMap("1", "v1", "2", "v2"))
                                .withMap2(asStringMap("", "v"))
                )
        );
    }
}
