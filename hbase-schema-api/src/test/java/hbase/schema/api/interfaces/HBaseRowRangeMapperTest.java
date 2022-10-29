package hbase.schema.api.interfaces;

import hbase.schema.api.models.HBaseRowRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class HBaseRowRangeMapperTest {

    @Test
    void bucketsMapper_DoesYieldRanges() {
        List<byte[]> buckets = asList(utf8("A"), utf8("Y"));
        List<HBaseRowRange> expectedRanges = asList(range("A", "B"), range("Y", "Z"));

        HBaseRowRangeMapper mapper = HBaseRowRangeMapper.bucketsMapper(buckets);
        assertThat(mapper.toRanges(emptyMap()), equalTo(expectedRanges));
    }

    @Test
    void constantMapper_DoesYieldRanges() {
        byte[] value = utf8("J");
        List<HBaseRowRange> expectedRanges = singletonList(range("J", "K"));

        HBaseRowRangeMapper mapper = HBaseRowRangeMapper.constantMapper(value);
        assertThat(mapper.toRanges(emptyMap()), equalTo(expectedRanges));
    }
//
//    static Stream<Arguments> provideBuckets() {
//        return Stream.of(
//                // buckets, params, expectedRanges
//                Arguments.of(
//                        asList(utf8("A"), utf8("Y")),
//                        emptyMap(),
//
//                        )
//        );
//    }

    static Stream<Arguments> provideConstants() {
        return Stream.of(
                // constant, params, expectedRanges
                Arguments.of(
                        utf8("J"),
                        emptyMap(),
                        singletonList(range("J", "K"))
                )
        );
    }

    static byte[] utf8(String s) {
        if (s != null) {
            return s.getBytes(StandardCharsets.UTF_8);
        } else {
            return null;
        }
    }

    static HBaseRowRange range(String start, String stop) {
        return new HBaseRowRange(utf8(start), utf8(stop));
    }
}
