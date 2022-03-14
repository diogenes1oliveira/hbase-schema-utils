package hbase.base.models;

import hbase.base.interfaces.TypeArg;

import java.util.List;

/**
 * Type specification for {@code List}
 */
@SuppressWarnings("rawtypes")
public class ListTypeArg implements TypeArg {
    private final Class itemType;

    private ListTypeArg(Class itemType) {
        this.itemType = itemType;
    }

    @Override
    public Class getTypeClass() {
        return List.class;
    }

    @Override
    public Class[] getTypeArgs() {
        return new Class[]{itemType};
    }

    @Override
    public String toString() {
        return "List<" + itemType + ">";
    }

    public static ListTypeArg listTypeArg(Class itemType) {
        return new ListTypeArg(itemType);
    }

}
