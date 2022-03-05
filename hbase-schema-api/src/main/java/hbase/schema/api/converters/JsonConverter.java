package hbase.schema.api.converters;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import hbase.schema.api.interfaces.conversion.BytesConverter;
import hbase.schema.api.interfaces.fields.HBaseBytesGetter;
import hbase.schema.api.interfaces.fields.HBaseBytesSetter;

import java.io.IOException;

/**
 * Converts objects as JSON strings
 *
 * @param <T> result type
 */
public class JsonConverter<T> implements BytesConverter<T> {
    private static volatile ObjectMapper defaultMapper = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Class<T> type;
    private final ObjectMapper mapper;

    /**
     * @param type   result type instance
     * @param mapper object mapper to use as SerDe
     */
    public JsonConverter(Class<T> type, ObjectMapper mapper) {
        this.type = type;
        this.mapper = mapper;
    }

    /**
     * Uses a default object mapper
     *
     * @param type result type instance
     */
    public JsonConverter(Class<T> type) {
        this(type, defaultMapper);
    }

    /**
     * Jsonifies the object
     */
    @Override
    public byte[] toBytes(T value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to serialize value", e);
        }
    }

    /**
     * Parses the object from a JSON payload
     */
    @Override
    public T fromBytes(byte[] bytes) {
        try {
            return mapper.readValue(bytes, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize value", e);
        }
    }

    /**
     * Result type
     */
    @Override
    public Class<?> type() {
        return type;
    }

    /**
     * Sets the default object mapper
     */
    public static void setDefaultMapper(ObjectMapper mapper) {
        defaultMapper = mapper;
    }

    /**
     * Convenience method aid in fluent APIs
     */
    public static <T> JsonConverter<T> jsonConverter(Class<T> type) {
        return new JsonConverter<>(type);
    }

    /**
     * Maps the object to a JSON value
     *
     * @param mapper ObjectMapper to use
     * @param <T>    object type
     * @return lambda to map the object to a {@code byte[]} JSON value
     */
    public static <T> HBaseBytesGetter<T> jsonGetter(ObjectMapper mapper) {
        return obj -> {
            try {
                return mapper.writeValueAsBytes(obj);
            } catch (IOException e) {
                throw new IllegalArgumentException("Couldn't deserialize the payload from JSON", e);
            }
        };
    }

    /**
     * Maps the object to a JSON value using the default ObjectMapper
     *
     * @param <T> object type
     * @return lambda to map the object to a {@code byte[]} JSON value
     */
    public static <T> HBaseBytesGetter<T> jsonGetter() {
        return jsonGetter(defaultMapper);
    }

    /**
     * Populates the object with data from a JSON value
     *
     * @param mapper ObjectMapper to use
     * @param <T>    object type
     * @return lambda to populate the object with a {@code byte[]} JSON value
     */
    public static <T> HBaseBytesSetter<T> jsonSetter(ObjectMapper mapper) {
        return (obj, bytes) -> {
            try {
                mapper.readerForUpdating(obj).readValue(bytes);
            } catch (IOException e) {
                throw new IllegalArgumentException("Couldn't deserialize the payload from JSON", e);
            }
        };
    }

    /**
     * Populates the object with data from a JSON value using the default ObjectMapper
     *
     * @param <T> object type
     * @return lambda to populate the object with a {@code byte[]} JSON value
     */
    public static <T> HBaseBytesSetter<T> jsonSetter() {
        return jsonSetter(defaultMapper);
    }

}
