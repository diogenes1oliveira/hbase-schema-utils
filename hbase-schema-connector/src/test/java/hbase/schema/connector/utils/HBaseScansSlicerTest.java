package hbase.schema.connector.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static hbase.schema.connector.utils.HBaseQueryUtils.toHBaseShell;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static testutils.ComparableScan.comparableScans;

class HBaseScansSlicerTest {
    @ParameterizedTest
    @MethodSource("provideScansToStop")
    void scanStopsBefore_WorksAsExpected(Scan input, Bytes rowKey, boolean expected) {
        boolean actual = HBaseScansSlicer.scanStopsBefore(input, rowKey.get());
        String message = String.format("scan %s stop before %s", toHBaseShell(input, "table"), rowKey);

        assertThat(message, actual, equalTo(expected));
    }

    @ParameterizedTest
    @MethodSource("provideScansToIntersect")
    void scanIntersects_WorksAsExpected(Scan input, Bytes rowKey, boolean expected) {
        boolean actual = HBaseScansSlicer.scanIntersects(input, rowKey.get());
        String message = String.format("scan %s intersects %s", toHBaseShell(input, "table"), rowKey);

        assertThat(message, actual, equalTo(expected));
    }

    @ParameterizedTest
    @MethodSource("provideScansToRemove")
    void removeBefore_WorksAsExpected(List<Scan> input, Bytes rowKey, List<Scan> expectedScans) {
        HBaseScansSlicer slicer = new HBaseScansSlicer(TableName.valueOf("table"), input);

        slicer.removeBefore(rowKey.get());
        List<Scan> actualScans = slicer.getScans();

        assertThat(comparableScans(actualScans), equalTo(comparableScans(expectedScans)));
    }

    public static Stream<Arguments> provideScansToRemove() {
        return Stream.of(
                Arguments.of(
                        asList(
                                new Scan().withStartRow(utf8("10d|v01|")).withStopRow(utf8("10d|v01}")),
                                new Scan().withStartRow(utf8("c8c|v01|")).withStopRow(utf8("c8c|v01}"))
                        ),
                        binary("10d|v01|01g1r7gnhv0008422ncjtv4xvh"),
                        asList(
                                new Scan().withStartRow(utf8("10d|v01|01g1r7gnhv0008422ncjtv4xvh")).withStopRow(utf8("10d|v01}")),
                                new Scan().withStartRow(utf8("c8c|v01|")).withStopRow(utf8("c8c|v01}"))
                        )
                )
        );
    }

    public static Stream<Arguments> provideScansToStop() {
        return Stream.of(
                Arguments.of(
                        new Scan().withStartRow(utf8("10d|v01|")).withStopRow(utf8("10d|v01}")),
                        binary("10d|v01|01g1r7gnhv0008422ncjtv4xvh"),
                        false
                )
        );
    }

    public static Stream<Arguments> provideScansToIntersect() {
        return Stream.of(
                Arguments.of(
                        new Scan().withStartRow(utf8("c8c|v01|")).withStopRow(utf8("c8c|v01")),
                        binary("c8c|v01|01g1r6d4nr0002cdyztfxwj197"),
                        true
                ),
                Arguments.of(
                        new Scan().withStartRow(utf8("10d|v01|")).withStopRow(utf8("10d|v01")),
                        binary("10d|v01|01g1r7gnhv0008422ncjtv4xvh"),
                        true
                ),
                Arguments.of(
                        new Scan().withStartRow(utf8("c8c|v01|")).withStopRow(utf8("c8c|v01")),
                        binary("10d|v01|01g1r7gnhv0008422ncjtv4xvh"),
                        false
                )
        );
    }

    private static Bytes binary(String s) {
        if (s == null) {
            return null;
        }
        return new Bytes(Bytes.toBytesBinary(s));
    }

    private static byte[] utf8(String s) {
        if (s == null) {
            return null;
        }
        return s.getBytes(StandardCharsets.UTF_8);
    }

}
