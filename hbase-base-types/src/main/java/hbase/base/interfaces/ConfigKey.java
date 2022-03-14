package hbase.base.interfaces;

import org.jetbrains.annotations.Nullable;

/**
 * Generic interface for a single config key
 */
public interface ConfigKey {
    /**
     * Config property name
     */
    String key();

    /**
     * Get a converted value for this config from the config object
     *
     * @param config config object
     * @param <T>    value type
     * @return converted value type
     */
    @Nullable <T> T fromConfig(Config config);
}
