package hbase.base.interfaces;

/**
 * Generic converter service
 *
 * @param <S> source type
 * @param <T> target type
 */
public interface Converter<S, T> extends Service {
    /**
     * Converts the source to the target type
     *
     * @param source source object
     * @return converted target object
     */
    T convertTo(S source);

    /**
     * Converts the target type to the source type
     *
     * @param target target object
     * @return converted source object
     */
    S convertFrom(T target);

    /**
     * Target type class object
     */
    Class<?> type();

    /**
     * Extra arguments to disambiguate the target type
     */
    default TypeArg[] typeArgs() {
        return new TypeArg[0];
    }

    /**
     * Checks if this converter type matches the desired conversion
     * <p>
     * The default implementation matches types via {@link Class#isAssignableFrom(Class)} and type args
     * via {@link TypeArg#isAssignableTo(TypeArg)}
     *
     * @param type     target type
     * @param typeArgs target type args
     * @return true if supported
     */
    default boolean canConvertTo(Class<?> type, TypeArg... typeArgs) {
        return type.isAssignableFrom(this.type()) && TypeArg.areAssignableTo(this.typeArgs(), typeArgs);
    }

}
