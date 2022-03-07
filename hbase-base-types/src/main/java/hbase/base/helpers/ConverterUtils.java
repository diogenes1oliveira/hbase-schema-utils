package hbase.base.helpers;

import hbase.base.interfaces.TypeArg;
import hbase.base.models.ListTypeArg;
import hbase.base.models.MapTypeArg;
import hbase.base.models.SimpleTypeArg;
import hbase.base.models.ValueTypeArg;

/**
 * Helper methods to deal with converters
 */
public final class ConverterUtils {
    private ConverterUtils() {
        // utility class
    }


    public static ValueTypeArg typeArg(Object value) {
        return new ValueTypeArg(value);
    }

    public static SimpleTypeArg typeArg(Class<?> classType) {
        return new SimpleTypeArg(classType);
    }

    public static ListTypeArg typeArg(TypeArg itemType) {
        return new ListTypeArg(itemType);
    }

    public static MapTypeArg typeArg(TypeArg keyType, TypeArg valueType) {
        return new MapTypeArg(keyType, valueType);
    }

    public static MapTypeArg typeArg(Class<?> keyType, Class<?> valueType) {
        return typeArg(typeArg(keyType), typeArg(valueType));
    }

}
