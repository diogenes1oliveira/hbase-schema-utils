package hbase.base.models;

import hbase.base.interfaces.TypeArg;

/**
 * Type specification for non-composite types
 */
public class SimpleTypeArg implements TypeArg {
    private final Class<?> type;

    /**
     * @param type type class object
     */
    public SimpleTypeArg(Class<?> type) {
        this.type = type;
    }

    /**
     * Returns {@code true} iff the target type is also a {@link SimpleTypeArg} and its type can be assigned from this
     */
    @Override
    public boolean isAssignableTo(TypeArg target) {
        if (!(target instanceof SimpleTypeArg)) {
            return false;
        }
        SimpleTypeArg other = (SimpleTypeArg) target;
        return other.type.isAssignableFrom(this.type);
    }

    @Override
    public String toString() {
        return type.getSimpleName() + ".class";
    }

}
