package hbase.base.helpers;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class ConfigUtilsTest {

    @ParameterizedTest
    @CsvSource({
            "ab, ab",
            "a.b.c, a.b.c",
            "12, 12",
            "a_b, a_b",
            "A_B, a.b",
            "A__B_C, a-b.c",
            "A___BC, aBc",
            "A____C, a_c",
            "'', ''"
    })
    void normalizeConfigName_MapsAsExpected(String input, String expected) {
        String actual = ConfigUtils.normalizeConfigName(input);
        assertThat(actual, equalTo(expected));
    }
}
