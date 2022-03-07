package hbase.base.helpers;

import hbase.base.interfaces.TypeArg;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static hbase.base.helpers.ConverterUtils.typeArg;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class ConverterUtilsTest {

    @ParameterizedTest
    @MethodSource("provideMatching")
    void matches_TrueWhenMatches(TypeArg converterType, TypeArg desiredType) {
        assertThat(converterType.isAssignableTo(desiredType), equalTo(true));
    }

    @ParameterizedTest
    @MethodSource("provideNonMatching")
    void matches_TrueWhenNoMatch(TypeArg converterType, TypeArg desiredType) {
        assertThat(converterType.isAssignableTo(desiredType), equalTo(false));
    }

    @ParameterizedTest
    @MethodSource("provideMatching")
    void matches_FalseWhenDummy(TypeArg converterType, TypeArg desiredType) {
        assertThat(TypeArg.DUMMY.isAssignableTo(converterType), equalTo(false));
        assertThat(TypeArg.DUMMY.isAssignableTo(desiredType), equalTo(false));

        assertThat(converterType.isAssignableTo(TypeArg.DUMMY), equalTo(false));
        assertThat(desiredType.isAssignableTo(TypeArg.DUMMY), equalTo(false));
    }

    static Stream<Arguments> provideMatching() {
        return Stream.of(
                Arguments.of(typeArg("1"), typeArg("1")),
                Arguments.of(typeArg(true), typeArg(true)),
                Arguments.of(typeArg(String.class), typeArg(String.class)),
                Arguments.of(typeArg(Long.class), typeArg(Number.class)),
                Arguments.of(typeArg(typeArg(Long.class)), typeArg(typeArg(Number.class))),
                Arguments.of(typeArg(typeArg(1 + 1)), typeArg(typeArg(2))),
                Arguments.of(typeArg(typeArg(byte[].class), typeArg(byte[].class)), typeArg(byte[].class, byte[].class))
        );
    }

    static Stream<Arguments> provideNonMatching() {
        return Stream.of(
                Arguments.of(typeArg("1"), typeArg(2L)),
                Arguments.of(typeArg(true), typeArg(false)),
                Arguments.of(typeArg(String.class), typeArg("string")),
                Arguments.of(typeArg(Number.class), typeArg(Long.class)),
                Arguments.of(typeArg(typeArg(Long.class)), typeArg("42")),
                Arguments.of(typeArg(Long.class, String.class), typeArg(Long.class, byte[].class))
        );
    }
}
