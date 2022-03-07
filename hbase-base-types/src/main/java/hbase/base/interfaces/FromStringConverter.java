package hbase.base.interfaces;

import hbase.base.services.ServiceRegistry;

import java.util.function.Function;

/**
 * Converter that handles conversions from strings
 *
 * @param <T> converted type
 */
public interface FromStringConverter<T> extends Converter<String, T> {
    /**
     * Gets a convert for the given specs using the global {@link ServiceRegistry}
     *
     * @param type     target type
     * @param typeArgs target type args
     * @param <T>      target class object
     * @return found converter
     * @throws IllegalArgumentException no converter available
     */
    @SuppressWarnings("unchecked")
    static <T> FromStringConverter<T> get(Class<?> type, TypeArg... typeArgs) {
        return ServiceRegistry.findService(FromStringConverter.class, c -> c.canConvertTo(type, typeArgs));
    }

    /**
     * Creates a new string converter from functional lambdas
     *
     * @param type     target type
     * @param parser   lambda to generate a converted value from a string
     * @param typeArgs extra type arguments
     * @param <T>      converted type
     * @return new string converter
     */
    static <T> FromStringConverter<T> fromStringConverter(Class<?> type, Function<String, T> parser, TypeArg... typeArgs) {
        return new FromStringConverter<T>() {
            @Override
            public T convertTo(String source) {
                return parser.apply(source);
            }

            @Override
            public String convertFrom(T target) {
                return target.toString();
            }

            @Override
            public Class<?> type() {
                return type;
            }

            @Override
            public TypeArg[] typeArgs() {
                return typeArgs;
            }
        };
    }
}
