package hbase.base.interfaces;

/**
 * Generic type argument specification
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@FunctionalInterface
public interface TypeArg {
    /**
     * Checks if this type argument can be assigned to the target
     *
     * @param target desired target type
     * @return true if this type arg matches
     */
    default boolean isAssignableTo(TypeArg target) {
        if (!target.getTypeClass().isAssignableFrom(this.getTypeClass())) {
            return false;
        }
        if(target.getTypeArgs().length != this.getTypeArgs().length) {
            return false;
        }
        for(int i = 0; i < getTypeArgs().length; ++i) {
            if(!target.getTypeArgs()[i].isAssignableFrom(this.getTypeArgs()[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Java class for this type
     */
    Class getTypeClass();

    /**
     * Extra arguments to convert to this type
     */
    default Class[] getTypeArgs() {
        return new Class[0];
    }

    /**
     * Convenience method to match arrays of type args
     *
     * @param sources available types
     * @param targets desired target type
     * @return true if same number of args, and all of them match in-order
     */
    static boolean areAssignableTo(TypeArg[] sources, TypeArg... targets) {
        if (sources.length != targets.length) {
            return false;
        }
        for (int i = 0; i < sources.length; ++i) {
            if (!sources[i].isAssignableTo(targets[i])) {
                return false;
            }
        }
        return true;
    }

}
