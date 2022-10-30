package dev.diogenes.hbase.schema.api.interfaces;

import static org.junit.Assert.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

public class BytesSlicerTest {
    @Test
    void fixed_ShouldConsumeEntireBuffer() {
        ByteBuffer input = buffer("something");

        ByteBuffer actual = BytesSlicer.full().slice(input).orElse(null);

        assertThat()
    }

    @Test
    void testFull() {

    }

    @Test
    void testSlice() {

    }

    @Test
    void testSplit() {

    }

    private static ByteBuffer buffer(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(bytes);
    }

    private static String string(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
