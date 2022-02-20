package hbase.schema.api;

import hbase.schema.api.utils.HBaseSchemaUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class HBaseSchemaUtilsTest {

    @ParameterizedTest
    @CsvSource(delimiterString = "|", value = {
            "ab, ac, ad | a",
            "012, 01324, 0145 | 01",
            "ab, ac, a | a"
    })
    void findCommonPrefix_DoesReturn(String commaInput, String expectedString) {
        List<byte[]> input = commasToBytes(commaInput);
        byte[] expected = expectedString.trim().getBytes(StandardCharsets.UTF_8);

        byte[] actual = HBaseSchemaUtils.findCommonPrefix(input);
        assertThat(actual, equalTo(expected));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ab, cd",
            "01, 02, 30"
    })
    void findCommonPrefix_NullIfNoCommonPrefix(String commaInput) {
        List<byte[]> input = commasToBytes(commaInput);
        assertThat(HBaseSchemaUtils.findCommonPrefix(input), nullValue());
    }

    private static List<byte[]> commasToBytes(String input) {
        return stream(input.split("[\\s,]+"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s.getBytes(StandardCharsets.UTF_8))
                .collect(toList());
    }
}
