package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseResultParser;
import hbase.schema.api.testutils.DummyPojo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.NavigableMap;
import java.util.stream.Stream;

import static hbase.schema.api.testutils.HBaseUtils.asStringMap;
import static hbase.schema.api.testutils.HBaseUtils.bytes;
import static hbase.schema.api.testutils.HBaseUtils.utf8;
import static hbase.schema.api.testutils.HBaseUtils.utf8Map;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class HBaseResultParserBuilderTest {
    HBaseResultParser<DummyPojo> resultParser = new HBaseResultParserBuilder<>(DummyPojo::new)
            .fromRowKey(DummyPojo::setId, utf8())
            .fromColumn("field", DummyPojo::setField, utf8())
            .fromPrefix("p", DummyPojo::setMap1, utf8Map())
            .fromPrefix("q", DummyPojo::setMap2, utf8Map())
            .build();

    @ParameterizedTest
    @MethodSource("provideCases")
    void doesParseAccordingly(byte[] rowKey, NavigableMap<byte[], byte[]> cellsMap, DummyPojo expected) {
        DummyPojo actual = new DummyPojo();
        resultParser.setFromRowKey(actual, rowKey);
        resultParser.setFromResult(actual, cellsMap);

        assertThat(actual, equalTo(expected));
    }

    static Stream<Arguments> provideCases() {
        return Stream.of(
                Arguments.of(
                        bytes("my row key"),
                        asBytesTreeMap(
                                bytes("field"), bytes("value"),
                                bytes("fieldOther"), bytes("other value"),
                                bytes("p1"), bytes("v1"),
                                bytes("p2"), bytes("v2"),
                                bytes("q"), bytes("v")
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
