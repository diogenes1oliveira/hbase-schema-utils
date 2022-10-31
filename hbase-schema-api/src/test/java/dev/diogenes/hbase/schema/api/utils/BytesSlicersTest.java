package dev.diogenes.hbase.schema.api.utils;

import dev.diogenes.hbase.schema.api.interfaces.BytesSlicer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import static dev.diogenes.hbase.schema.api.testutils.BufferUtils.buffer;
import static dev.diogenes.hbase.schema.api.testutils.BufferUtils.string;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class BytesSlicersTest {
    @ParameterizedTest
    @MethodSource("provideFull")
    void testFull(ByteBuffer input, String expected, String remaining) {
        ByteBuffer actual = BytesSlicers.full().slice(input).orElse(null);

        assertThat(string(actual), equalTo(expected));
        assertThat(string(input), equalTo(remaining));
    }

    @ParameterizedTest
    @MethodSource("provideFixed")
    void testFixed(ByteBuffer input, int sliceSize, String expected, String remaining) {
        ByteBuffer actual = BytesSlicers.fixed(sliceSize).slice(input).orElse(null);

        assertThat(string(actual), equalTo(expected));
        assertThat(string(input), equalTo(remaining));
    }

    @ParameterizedTest
    @MethodSource("provideSplit")
    void testSplit(ByteBuffer input, int separator, String expected, String remaining) {
        ByteBuffer actual = BytesSlicers.split((byte) separator).slice(input).orElse(null);

        assertThat(string(actual), equalTo(expected));
        assertThat(string(input), equalTo(remaining));
    }

    @Test
    void testToSlices() {
        ByteBuffer input = buffer("a|bb|ccc");

        List<BytesSlicer> slicers = asList(
                BytesSlicers.split('|'),
                BytesSlicers.split('|'),
                BytesSlicers.split('|')
        );

        List<ByteBuffer> slices = BytesSlicers.toSlices(input, slicers);

        assertThat(slices.size(), equalTo(3));
        assertThat(input.hasRemaining(), equalTo(false));

        assertThat(string(slices.get(0)), equalTo("a"));
        assertThat(string(slices.get(1)), equalTo("bb"));
        assertThat(string(slices.get(2)), equalTo("ccc"));
    }

    static Stream<Arguments> provideFull() {
        return Stream.of(
                Arguments.of(buffer(""), "", ""),
                Arguments.of(buffer("something"), "something", ""),
                Arguments.of(buffer("part").position(1), "art", "")
        );
    }

    static Stream<Arguments> provideFixed() {
        return Stream.of(
                Arguments.of(buffer(""), 2, null, ""),
                Arguments.of(buffer("a"), 2, null, ""),
                Arguments.of(buffer("parts").position(1), 2, "ar", "ts")
        ).skip(1).limit(1);
    }

    static Stream<Arguments> provideSplit() {
        return Stream.of(
                Arguments.of(buffer(""), 2, "", ""),
                Arguments.of(buffer(""), 5, "", ""),
                Arguments.of(buffer("par|ts").position(1), '|', "ar", "ts")
        );
    }

}
