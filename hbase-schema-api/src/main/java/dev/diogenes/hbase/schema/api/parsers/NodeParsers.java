package dev.diogenes.hbase.schema.api.parsers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import dev.diogenes.hbase.schema.api.interfaces.NodeParser;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

public final class NodeParsers {
    private NodeParsers() {
        // utility class
    }

    public static NodeParser stringNodeParser(Function<ByteBuffer, String> toString) {
        return buffer -> {
            String s = toString.apply(buffer);
            return JsonNodeFactory.instance.textNode(s);
        };
    }

    public static NodeParser utf8NodeParser() {
        return stringNodeParser(buffer -> StandardCharsets.UTF_8.decode(buffer).toString());
    }

    public static NodeParser hexNodeParser() {
        return stringNodeParser(Hex::encodeHexString);
    }

    public static NodeParser base64NodeParser() {
        return stringNodeParser(buffer -> {
            byte[] bytes = Bytes.toBytes(buffer);
            return Base64.encodeBase64String(bytes);
        });
    }

    public static NodeParser stringBinaryNodeParser() {
        return stringNodeParser(Bytes::toStringBinary);
    }

    public static NodeParser longNodeParser() {
        return buffer -> {
            byte[] bytes = Bytes.toBytes(buffer);
            long value = Bytes.toLong(bytes);

            return JsonNodeFactory.instance.numberNode(value);
        };
    }

    public static NodeParser jsonNodeParser(ObjectMapper mapper) {
        return buffer -> {
            byte[] bytes = Bytes.toBytes(buffer);

            try {
                return mapper.readTree(bytes);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        };
    }


}
