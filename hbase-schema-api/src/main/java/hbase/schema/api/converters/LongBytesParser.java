package hbase.schema.api.converters;

import hbase.schema.api.interfaces.BytesParser;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class LongBytesParser implements BytesParser<Long> {
    @Override
    public Long parse(@NotNull ByteBuffer buffer) {
        if (buffer.remaining() != 8) {
            return null;
        }
        return buffer.getLong();
    }
}
