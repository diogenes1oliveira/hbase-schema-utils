package hbase.base.interfaces;

import hbase.base.services.PropertiesConfig;
import org.jetbrains.annotations.Nullable;

import java.util.Properties;
import java.util.function.Function;

/**
 * Generic interface for config objects.
 * <p>
 * This will likely be specialized to a framework such as Spring or Quarkus. If not, there's the reference
 * implementation {@link PropertiesConfig} for a System properties-based config.
 */
@SuppressWarnings("unchecked")
public interface Config {
    /**
     * Gets the config specified by the given key
     *
     * @param namedType
     * @param <T>
     * @return
     */
    default <T> T get(NamedType namedType) {
        return namedType.getFromConfig(this);
    }

    /**
     * Gets all configurations with the given prefix
     *
     * @param prefix config prefix
     * @return all prefixed configurations
     */
    Properties getPrefix(String prefix);

    /**
     * Gets a converted value for the configuration
     * <p>
     * The default implementation gets a string value using {@link #getValue(String)} and then converts it with
     * {@link FromStringConverter#get(Class, TypeArg...)}
     *
     * @param name     config name
     * @param type     config type
     * @param typeArgs extra arguments to disambiguate the type
     * @param <T>      configuration concrete type
     * @return converted value
     */
    default @Nullable <T> T getValue(String name, Class<?> type, TypeArg... typeArgs) {
        return getValue(name, null, type, typeArgs);
    }

    /**
     * Gets a converted value for the configuration
     * <p>
     * The default implementation gets a string value using {@link #getValue(String)} and then converts it with
     * {@link FromStringConverter#get(Class, TypeArg...)}
     *
     * @param name     config name
     * @param type     config type
     * @param typeArgs extra arguments to disambiguate the type
     * @param <T>      configuration concrete type
     * @return converted value
     */
    default <T> T getValue(String name, T defaultValue, Class<?> type, TypeArg... typeArgs) {
        if(type == Properties.class) {
            return (T) getPrefix(name);
        }
        FromStringConverter<T> converter = FromStringConverter.get(type, typeArgs);
        return getValue(name, defaultValue, converter::convertTo);
    }

    /**
     * Gets a stringified value for the config
     *
     * @param name config name
     * @return config value
     */
    @Nullable String getValue(String name);

    /**
     * Gets a converted value for the config
     * <p>
     * The default implementation delegates to {@link #getValue(String)}
     *
     * @param name      config name
     * @param converter maps a string to another type
     * @return converted config value
     */
    default @Nullable <T> T getValue(String name, Function<String, T> converter) {
        return getValue(name, null, converter);
    }

    /**
     * Gets a converted value for the config
     * <p>
     * The default implementation delegates to {@link #getValue(String)}
     *
     * @param name         config name
     * @param converter    maps a string to another type
     * @param defaultValue default value if null
     * @return converted config value
     */
    default <T> T getValue(String name, T defaultValue, Function<String, T> converter) {
        String value = getValue(name);
        if (value == null) {
            return defaultValue;
        }
        T converted = converter.apply(value);
        if (converted == null) {
            return defaultValue;
        }
        return converted;
    }

}
