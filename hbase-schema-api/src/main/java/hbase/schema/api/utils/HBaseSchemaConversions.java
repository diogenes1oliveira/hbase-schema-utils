package hbase.schema.api.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import hbase.schema.api.interfaces.converters.HBaseBytesGetter;
import hbase.schema.api.interfaces.converters.HBaseBytesMapGetter;
import hbase.schema.api.interfaces.converters.HBaseBytesMapSetter;
import hbase.schema.api.interfaces.converters.HBaseBytesSetter;
import hbase.schema.api.interfaces.converters.HBaseLongGetter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static hbase.schema.api.interfaces.converters.HBaseBytesGetter.bytesGetter;
import static hbase.schema.api.interfaces.converters.HBaseBytesMapSetter.bytesMapSetter;

/**
 * Generic utility aid methods
 */
public final class HBaseSchemaConversions {
    private static final Long LONG_ONE = 1L;

    private HBaseSchemaConversions() {
        // utility class
    }

    /**
     * Encodes the string as UTF-8 bytes
     */
    public static byte[] utf8ToBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Function to encode the string as UTF-8 bytes
     */
    public static HBaseBytesGetter<String> utf8ToBytes() {
        return HBaseSchemaConversions::utf8ToBytes;
    }

    /**
     * Decodes the UTF-8 bytes into a string
     */
    public static String utf8FromBytes(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    /**
     * Function to decode the string from UTF-8 bytes
     */
    public static Function<byte[], String> utf8FromBytes() {
        return HBaseSchemaConversions::utf8FromBytes;
    }

    /**
     * Gets a UTF-8 string from an object
     *
     * @param getter lambda to get a String value from the object
     * @param <O>    object type
     * @return lambda to extract a {@code byte[]} value from the object
     */
    public static <O> HBaseBytesGetter<O> stringGetter(Function<O, String> getter) {
        return bytesGetter(getter, HBaseSchemaConversions::utf8ToBytes);
    }

    /**
     * Gets a field encoded as a UTF-8 string from an object
     *
     * @param getter    lambda to get a field value from the object
     * @param converter converts the field into a string
     * @param <O>       object type
     * @param <F>       field type
     * @return lambda to extract a {@code byte[]} value from the object
     */
    public static <O, F> HBaseBytesGetter<O> stringGetter(Function<O, F> getter, Function<F, String> converter) {
        return obj -> {
            F value = getter.apply(obj);
            return value != null ? converter.apply(value).getBytes(StandardCharsets.UTF_8) : null;
        };
    }

    /**
     * Maps the object to a JSON value
     *
     * @param mapper Jackson mapper
     * @param <O>    object type
     * @return lambda to map the object to a {@code byte[]} JSON value
     */
    public static <O> HBaseBytesGetter<O> jsonGetter(ObjectMapper mapper) {
        return obj -> {
            try {
                return mapper.writeValueAsBytes(obj);
            } catch (IOException e) {
                throw new IllegalArgumentException("Couldn't deserialize the payload from JSON", e);
            }
        };
    }

    /**
     * Gets a boolean field in the object
     *
     * @param getter lambda to get the boolean field
     * @param <O>    object type
     * @return lambda to extract a Long value from the object: null for false and 1 for true
     */
    public static <O> HBaseLongGetter<O> booleanGetter(Function<O, Boolean> getter) {
        return obj -> {
            Boolean value = getter.apply(obj);
            if (value == null) {
                return null;
            }
            return value ? LONG_ONE : null;
        };
    }

    /**
     * Gets a boolean field in the object
     *
     * @param getter    lambda to get the field from the object
     * @param converter converts the field into a Boolean
     * @param <O>       object type
     * @param <F>       field type
     * @return lambda to extract a Long value from the object: null for false and 1 for true
     */
    public static <O, F> HBaseLongGetter<O> booleanGetter(Function<O, F> getter, Function<F, Boolean> converter) {
        return booleanGetter(obj -> {
            F value = getter.apply(obj);
            if (value == null) {
                return null;
            }
            return converter.apply(value);
        });
    }

    /**
     * Maps a list field in the object to a set of HBase cells
     *
     * @param listGetter         gets the list field from the object
     * @param qualifierConverter converts the item to a qualifier
     * @param itemConverter      converts the item to a cell value
     * @param <T>                object type
     * @param <I>                item type
     * @return lambda to map the object field to a map (qualifier -> cell value)
     */
    public static <T, I> HBaseBytesMapGetter<T> listColumnGetter(
            Function<T, List<? extends I>> listGetter,
            HBaseBytesGetter<I> qualifierConverter,
            HBaseBytesGetter<I> itemConverter
    ) {
        return obj -> {
            NavigableMap<byte[], byte[]> cellsMap = HBaseSchemaUtils.asBytesTreeMap();
            List<? extends I> list = listGetter.apply(obj);
            if (list == null || list.isEmpty()) {
                return cellsMap;
            }
            for (I item : list) {
                byte[] value = itemConverter.getBytes(item);
                byte[] qualifier = qualifierConverter.getBytes(item);
                if (value != null && qualifier != null) {
                    cellsMap.put(qualifier, value);
                }
            }
            return cellsMap;
        };
    }

    /**
     * Sets a String field in the object
     *
     * @param setter lambda to set the object field
     * @param <O>    object type
     * @return lambda to populate the object from a {@code byte[]} value
     */
    public static <O> HBaseBytesSetter<O> stringSetter(BiConsumer<O, String> setter) {
        return (obj, bytes) -> {
            String value = new String(bytes, StandardCharsets.UTF_8);
            setter.accept(obj, value);
        };
    }

    /**
     * Sets a String-conversible field in the object
     *
     * @param setter    lambda to set the object field
     * @param converter lambda to convert the string into the field type
     * @param <O>       object type
     * @param <F>       field type
     * @return lambda to populate the object from a {@code byte[]} value
     */
    public static <O, F> HBaseBytesSetter<O> stringSetter(BiConsumer<O, F> setter, Function<String, F> converter) {
        return (obj, bytes) -> {
            String value = new String(bytes, StandardCharsets.UTF_8);
            setter.accept(obj, converter.apply(value));
        };
    }

    /**
     * Sets a String map field in the object
     *
     * @param setter lambda to set the object map field
     * @return lambda to populate the object from a {@code Map<byte[], byte[]>} value
     */
    public static <O> HBaseBytesMapSetter<O> stringMapSetter(BiConsumer<O, Map<String, String>> setter) {
        return bytesMapSetter(
                setter,
                HBaseSchemaConversions::utf8FromBytes,
                HBaseSchemaConversions::utf8FromBytes
        );
    }

    /**
     * Populates the object with data from a JSON column
     *
     * @param mapper Jackson mapper
     * @param <O>    object type
     * @return lambda to populate the object with a {@code byte[]} JSON value
     */
    public static <O> HBaseBytesSetter<O> jsonSetter(ObjectMapper mapper) {
        return (obj, bytes) -> {
            try {
                mapper.readerForUpdating(obj).readValue(bytes);
            } catch (IOException e) {
                throw new IllegalArgumentException("Couldn't deserialize the payload from JSON", e);
            }
        };
    }

    /**
     * Sets a Long field in the object
     *
     * @param setter lambda to set the object field
     * @param <O>    object type
     * @return lambda to populate the object from a {@code byte[]} value
     */
    public static <O> HBaseBytesSetter<O> longSetter(BiConsumer<O, Long> setter) {
        return (obj, bytes) -> {
            Long value = Bytes.toLong(bytes);
            setter.accept(obj, value);
        };
    }

    /**
     * Sets a Long field in the object
     *
     * @param setter    lambda to set the object field
     * @param converter lambda to convert the Long value into the field type
     * @param <O>       object type
     * @param <F>       field type
     * @return lambda to populate the object from a {@code byte[]} value
     */
    public static <O, F> HBaseBytesSetter<O> longSetter(BiConsumer<O, F> setter, Function<Long, F> converter) {
        return longSetter((obj, l) -> {
            F value = converter.apply(l);
            setter.accept(obj, value);
        });
    }

    /**
     * Populates a list field in the object from a set of HBase cells
     *
     * @param listSetter      sets the list field in the object
     * @param itemBytesSetter populates the item with the cell value
     * @param itemConstructor creates a new item instance
     * @param <T>             object type
     * @param <I>             item type
     * @return lambda to populate the object list field from a map (qualifier -> cell value)
     */
    public static <T, I> HBaseBytesMapSetter<T> listColumnSetter(
            BiConsumer<T, List<I>> listSetter,
            HBaseBytesSetter<I> itemBytesSetter,
            Supplier<I> itemConstructor
    ) {
        return listColumnSetter(listSetter, bytes -> {
            I item = itemConstructor.get();
            itemBytesSetter.setFromBytes(item, bytes);
            return item;
        });
    }

    /**
     * Populates a list field in the object from a set of HBase cells
     *
     * @param listSetter           sets the list field in the object
     * @param itemBytesConstructor creates a new item from a cell value
     * @param <T>                  object type
     * @param <I>                  item type
     * @return lambda to populate the object list field from a map (qualifier -> cell value)
     */
    public static <T, I> HBaseBytesMapSetter<T> listColumnSetter(
            BiConsumer<T, List<I>> listSetter,
            Function<byte[], I> itemBytesConstructor
    ) {
        return (obj, bytesMap) -> {
            List<I> itens = new ArrayList<>();

            for (byte[] value : bytesMap.values()) {
                I item = itemBytesConstructor.apply(value);
                itens.add(item);
            }

            listSetter.accept(obj, itens);
        };
    }
}
