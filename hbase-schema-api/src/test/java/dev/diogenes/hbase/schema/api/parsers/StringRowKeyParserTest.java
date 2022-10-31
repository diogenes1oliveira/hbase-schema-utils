package dev.diogenes.hbase.schema.api.parsers;

import dev.diogenes.hbase.schema.api.utils.BytesSlicers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static dev.diogenes.hbase.schema.api.testutils.BufferUtils.buffer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class StringRowKeyParserTest {

    @Test
    void parseRowKey_HandlesInvalidParseResults() {
        StringRowKeyParser parser = new StringRowKeyParser(
                BytesSlicers.split('|'),
                BytesSlicers.remainder()
        );
        String input = "single";

        List<String> result = new ArrayList<>(singletonList("something"));
        assertThat(parser.parseRowKey(result, buffer(input)), equalTo(false));

        assertThat(result.size(), equalTo(1));
    }


    @Test
    void parseRowKey_SplitsBySeparator() {
        StringRowKeyParser parser = new StringRowKeyParser(
                BytesSlicers.split('|'),
                BytesSlicers.split('|'),
                BytesSlicers.remainder()
        );
        String input = "left|middle|right";

        List<String> result = parser.newInstance();
        assertThat(parser.parseRowKey(result, buffer(input)), equalTo(true));

        assertThat(result, equalTo(asList("left", "middle", "right")));
    }

    @Test
    void parseRowKey_CanMixSeparatorAndFixedSlice() {
        StringRowKeyParser parser = new StringRowKeyParser(
                BytesSlicers.fixed(3),
                BytesSlicers.split('|'),
                BytesSlicers.remainder()
        );
        String input = "123middle|right";

        List<String> result = parser.newInstance();
        assertThat(parser.parseRowKey(result, buffer(input)), equalTo(true));

        assertThat(result, equalTo(asList("123", "middle", "right")));
    }
}
