package hbase.base.models;

import hbase.base.interfaces.TypeArg;

import java.util.Map;

/**
 * Type specification for {@code Map}
 */
@SuppressWarnings("rawtypes")
public class MapTypeArg implements TypeArg {
    private final Class keyType;
    private final Class valueType;

    private MapTypeArg(Class keyType, Class valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public Class<?> getTypeClass() {
        return Map.class;
    }

    @Override
    public Class[] getTypeArgs() {
        return new Class[]{keyType, valueType};
    }

    @Override
    public String toString() {
        return "Map<" + keyType + ", " + valueType + ">";
    }

    public static MapTypeArg mapTypeArg(Class keyType, Class valueType) {
        return new MapTypeArg(keyType, valueType);
    }

}
