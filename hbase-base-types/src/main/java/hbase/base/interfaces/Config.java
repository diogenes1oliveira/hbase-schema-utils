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
public interface Config {
    /**
     * Gets the config specified by the given key
     * @param configKey
     * @param <T>
     * @return
     */
    default <T> T get(ConfigKey configKey) {
        return configKey.fromConfig(this);
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
        String stringValue = getValue(name);
        if (stringValue == null) {
            return null;
        }
        FromStringConverter<T> converter = FromStringConverter.get(type, typeArgs);
        return converter.convertTo(stringValue);
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
        String value = getValue(name);
        return value == null ? null : converter.apply(value);
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
    default <T> T getValue(String name, Function<String, T> converter, T defaultValue) {
        String value = getValue(name);
        return value == null ? defaultValue : converter.apply(value);
    }

}
