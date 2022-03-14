package hbase.base.models;

import hbase.base.interfaces.TypeArg;

/**
 * Type specification for non-composite types
 */
@SuppressWarnings({"rawtypes"})
public class SimpleTypeArg implements TypeArg {
    private final Class type;

    private SimpleTypeArg(Class type) {
        this.type = type;
    }

    @Override
    public Class getTypeClass() {
        return type;
    }

    @Override
    public String toString() {
        return type.getSimpleName();
    }

    public static SimpleTypeArg typeArg(Class type) {
        return new SimpleTypeArg(type);
    }

}
