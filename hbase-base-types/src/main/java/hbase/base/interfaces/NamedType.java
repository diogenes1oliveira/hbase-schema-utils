package hbase.base.interfaces;

public interface NamedType {
    String getName();

    /**
     * Java class for this type
     */
    default Class<?> getTypeClass() {
        return String.class;
    }

    /**
     * Extra arguments to convert to this type
     */
    default TypeArg[] getTypeArgs() {
        return new TypeArg[0];
    }

    default Object getDefaultValue() {
        return null;
    }

    default boolean isNullable() {
        return false;
    }

    /**
     * Checks if this type argument can be assigned to the target
     *
     * @param target desired target type
     * @return true if this type arg matches
     */
    default boolean isAssignableTo(NamedType target) {
        return false;
    }

    @SuppressWarnings("unchecked")
    default <T> T getFromConfig(Config config) {
        String name = getName();
        T value = config.getValue(name, getTypeClass(), getTypeArgs());
        if (value != null) {
            return value;
        }
        if (!isNullable()) {
            throw new IllegalArgumentException("No value for required option " + name);
        } else {
            return (T) getDefaultValue();
        }

    }
}
