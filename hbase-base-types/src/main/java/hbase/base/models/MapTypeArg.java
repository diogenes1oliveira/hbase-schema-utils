package hbase.base.models;

import hbase.base.interfaces.TypeArg;

/**
 * Type specification for {@code Map}
 */
public class MapTypeArg implements TypeArg {
    private final TypeArg keyType;
    private final TypeArg valueType;

    /**
     * @param keyType   generic key type
     * @param valueType generic value type
     */
    public MapTypeArg(TypeArg keyType, TypeArg valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    /**
     * Returns {@code true} iff the target type is also a {@code Map} and the key and value types match
     */
    @Override
    public boolean isAssignableTo(TypeArg target) {
        if (!(target instanceof MapTypeArg)) {
            return false;
        }
        MapTypeArg other = (MapTypeArg) target;
        return this.keyType.isAssignableTo(other.keyType) && this.valueType.isAssignableTo(other.keyType);
    }

    @Override
    public String toString() {
        return "Map<" + keyType + ", " + valueType + ">";
    }

}
