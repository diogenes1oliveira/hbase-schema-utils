package hbase.schema.api.converters;

import hbase.schema.api.interfaces.BytesParser;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

public class Utf8BytesParser implements BytesParser<String> {
    @Override
    public String parse(@NotNull ByteBuffer buffer) {
        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
        return charBuffer.toString();
    }

    public static final Utf8BytesParser UTF8_BYTES_PARSER = new Utf8BytesParser();
}
