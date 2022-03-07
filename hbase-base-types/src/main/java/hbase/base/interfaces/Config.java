package hbase.base.interfaces;

import hbase.base.helpers.ConfigUtils;
import hbase.base.services.PropertiesConfig;
import org.jetbrains.annotations.Nullable;

import java.util.Properties;

/**
 * Generic interface for config objects.
 * <p>
 * This will likely be specialized to a framework such as Spring or Quarkus. If not, there's the reference
 * implementation {@link PropertiesConfig} for a System properties-based config.
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
     * <p>
     * The default implementation delegates to {@link #getValue(String, Class, TypeArg...)}
     *
     * @param name config name
     * @return config value
     */
    @Nullable String getValue(String name);

    /**
     * Checks if the config exists
     * <p>
     * The default implementation calls {@link #getValue(String)}, considering {@code null} as a non-existing config
     *
     * @param name config name
     * @return result
     */
    default boolean hasConfig(String name) {
        return getValue(name) != null;
    }

    /**
     * Configures the instance with the values from this config
     * <p>
     * The default implementation delegates to {@link ConfigUtils#configure(Configurable, Config, String)}
     *
     * @param configurable instance to be configured
     */
    default void configure(Configurable configurable) {
        for (String configSpec : configurable.configs()) {
            ConfigUtils.configure(configurable, this, configSpec);
        }
    }
}
