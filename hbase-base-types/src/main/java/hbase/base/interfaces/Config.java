package hbase.base.interfaces;

import org.jetbrains.annotations.Nullable;

import java.util.Properties;
import java.util.function.Function;

/**
 * Generic interface for config objects.
 * <p>
 * This will likely be specialized to a framework such as Spring or Quarkus
 */
public interface Config {

    /**
     * Gets all configurations with the given prefix
     *
     * @param prefix config prefix
     * @return all prefixed configurations
     */
    Properties getPrefix(String prefix);

    /**
     * Gets a stringified value for the config
     *
     * @param name config name
     * @return config value
     */
    @Nullable String getValue(String name);

    /**
     * Gets a converted value for the config
     *
     * @param name config name
     * @param type target type object
     * @return config value
     */
    @Nullable <T> T getValue(String name, Class<?> type);

    /**
     * Gets a value from the given config key in a type-safe fashion
     *
     * @param configKey    config to get a value for
     * @param defaultValue default value in case of null
     * @param type         type class object
     * @param <T>          value type
     * @return converted config value
     */
    @SuppressWarnings("unchecked")
    default <T> T getValue(ConfigKey configKey, T defaultValue, Class<T> type) {
        Object value = configKey.fromConfig(this);
        if (value == null) {
            return defaultValue;
        }
        if (!type.isAssignableFrom(value.getClass())) {
            throw new IllegalArgumentException("Unexpected type for " + configKey.key());
        }
        return (T) value;
    }

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
        if (value == null) {
            return null;
        }
        return converter.apply(value);
    }

}
