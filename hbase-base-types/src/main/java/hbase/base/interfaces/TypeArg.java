package hbase.base.interfaces;

/**
 * Generic type argument specification
 */
@FunctionalInterface
public interface TypeArg {
    /**
     * Checks if this type argument can be assigned to the target
     *
     * @param target desired target type
     * @return true if this type arg matches
     */
    boolean isAssignableTo(TypeArg target);

    /**
     * Convenience method to match arrays of type args
     *
     * @param types   available types
     * @param targets desired target type
     * @return true if same number of args, and all of them match in-order
     */
    static boolean areAssignableTo(TypeArg[] types, TypeArg... targets) {
        if (types.length != targets.length) {
            return false;
        }
        for (int i = 0; i < types.length; ++i) {
            if (!types[i].isAssignableTo(targets[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Dummy type arg that never matches anything
     */
    TypeArg DUMMY = target -> false;
}
