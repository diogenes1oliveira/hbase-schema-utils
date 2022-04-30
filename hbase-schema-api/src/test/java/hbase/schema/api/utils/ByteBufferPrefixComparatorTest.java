package hbase.schema.api.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class ByteBufferPrefixComparatorTest {
    @ParameterizedTest
    @CsvSource({
            "b, a, 1",
            "a, b, -1",
            "a, a, 0",
            "a:0, a, 0",
            "'', '', 0",
            "'', a, 0",
    })
    void compare_WorksAsExpected(String s1, String s2, int expectedResult) {
        ByteBuffer b1 = utf8(s1);
        ByteBuffer b2 = utf8(s2);

        int actualResult = ByteBufferPrefixComparator.INSTANCE.compare(b1, b2);
        assertThat(actualResult, equalTo(expectedResult));
    }

    private static ByteBuffer utf8(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

}
