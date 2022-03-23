package hbase.schema.api.converters;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Base class to map lists into JSON maps
 *
 * @param <T> item type
 */
public abstract class ListMapAbstractJsonConverter<T> extends ListMapAbstractConverter<T> {
    @SuppressWarnings("java:S3077")
    private static volatile ObjectMapper defaultMapper = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Class<T> itemType;
    private final ObjectMapper mapper;

    /**
     * @param itemType result type instance
     * @param mapper   object mapper to use as SerDe
     */
    protected ListMapAbstractJsonConverter(Class<T> itemType, ObjectMapper mapper) {
        this.itemType = itemType;
        this.mapper = mapper;
    }

    /**
     * Uses a default object mapper
     *
     * @param itemType result type instance
     */
    protected ListMapAbstractJsonConverter(Class<T> itemType) {
        this(itemType, defaultMapper);
    }

    /**
     * Converts the item to a String key
     */
    public abstract @Nullable String toStringKey(int index, T item);

    /**
     * Encodes as UTF-8 the string returned by {@link #toStringKey(int, Object)}
     */
    @Override
    public byte @Nullable [] toBytesKey(int index, T item) {
        String key = toStringKey(index, item);
        return key == null ? null : key.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Jsonifies the item
     */
    @Override
    public byte @Nullable [] toBytesValue(int index, T item) {
        try {
            return mapper.writeValueAsBytes(item);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to serialize item", e);
        }
    }

    /**
     * Parses the item from JSON
     */
    @Override
    public @Nullable T fromBytes(byte[] key, byte[] value) {
        try {
            return mapper.readValue(value, itemType);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to serialize item", e);
        }
    }

    /**
     * Sets the default object mapper
     */
    public static void setDefaultMapper(ObjectMapper mapper) {
        defaultMapper = mapper;
    }

    /**
     * Creates a new list to map converter for the given item type
     *
     * @param itemType    item type instance
     * @param toStringKey function to map an item to a String key
     * @param <T>         item type
     * @return a new list converter
     */
    public static <T> ListMapAbstractJsonConverter<T> listMapJsonConverter(Class<T> itemType, Function<T, String> toStringKey) {
        return new ListMapAbstractJsonConverter<T>(itemType) {
            @Override
            public @Nullable String toStringKey(int index, T item) {
                return toStringKey.apply(item);
            }
        };
    }

    /**
     * Creates a new list to map converter for the given item type
     *
     * @param itemType    item type instance
     * @param toStringKey function to map (index, item) to a String key
     * @param <T>         item type
     * @return a new list converter
     */
    public static <T> ListMapAbstractJsonConverter<T> listMapJsonConverter(Class<T> itemType, BiFunction<Integer, T, String> toStringKey) {
        return new ListMapAbstractJsonConverter<T>(itemType) {
            @Override
            public @Nullable String toStringKey(int index, T item) {
                return toStringKey.apply(index, item);
            }
        };
    }

    /**
     * Creates a new list to map converter for the given item type
     * <p>
     * The keys are generated as the strings "0", "1", "2", ...
     *
     * @param itemType item type instance
     * @param <T>      item type
     * @return a new list converter
     */
    public static <T> ListMapAbstractJsonConverter<T> listMapJsonConverter(Class<T> itemType) {
        return new ListMapAbstractJsonConverter<T>(itemType) {
            @Override
            public String toStringKey(int index, T item) {
                return Integer.toString(index);
            }
        };
    }

    /**
     * Creates a new list to map converter for the given item type
     *
     * @param itemType    item type instance
     * @param toStringKey function to map an item to a String key
     * @param <T>         item type
     * @return a new list converter
     */
    public static <T> ListMapAbstractJsonConverter<T> listMapJsonConverter(Class<T> itemType,
                                                                           ObjectMapper mapper,
                                                                           Function<T, String> toStringKey) {
        return new ListMapAbstractJsonConverter<T>(itemType, mapper) {
            @Override
            public @Nullable String toStringKey(int index, T item) {
                return toStringKey.apply(item);
            }
        };
    }

    /**
     * Creates a new list to map converter for the given item type
     *
     * @param itemType    item type instance
     * @param toStringKey function to map (index, item) to a String key
     * @param <T>         item type
     * @return a new list converter
     */
    public static <T> ListMapAbstractJsonConverter<T> listMapJsonConverter(Class<T> itemType,
                                                                           ObjectMapper mapper,
                                                                           BiFunction<Integer, T, String> toStringKey) {
        return new ListMapAbstractJsonConverter<T>(itemType, mapper) {
            @Override
            public @Nullable String toStringKey(int index, T item) {
                return toStringKey.apply(index, item);
            }
        };
    }

    /**
     * Creates a new list to map converter for the given item type
     * <p>
     * The keys are generated as the strings "0", "1", "2", ...
     *
     * @param itemType item type instance
     * @param <T>      item type
     * @return a new list converter
     */
    public static <T> ListMapAbstractJsonConverter<T> listMapJsonConverter(Class<T> itemType,
                                                                           ObjectMapper mapper) {
        return new ListMapAbstractJsonConverter<T>(itemType, mapper) {
            @Override
            public String toStringKey(int index, T item) {
                return Integer.toString(index);
            }
        };
    }

}
