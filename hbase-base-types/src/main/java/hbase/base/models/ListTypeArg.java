package hbase.base.models;

import hbase.base.interfaces.TypeArg;

/**
 * Type specification for {@code List}
 */
public class ListTypeArg implements TypeArg {
    private final TypeArg itemType;

    /**
     * @param itemType generic item type
     */
    public ListTypeArg(TypeArg itemType) {
        this.itemType = itemType;
    }

    /**
     * Returns {@code true} iff the target type is also a {@code List} and the item types match
     */
    @Override
    public boolean isAssignableTo(TypeArg target) {
        if (!(target instanceof ListTypeArg)) {
            return false;
        }
        ListTypeArg other = (ListTypeArg) target;
        return this.itemType.isAssignableTo(other.itemType);
    }

    @Override
    public String toString() {
        return "List<" + itemType + ">";
    }

}
