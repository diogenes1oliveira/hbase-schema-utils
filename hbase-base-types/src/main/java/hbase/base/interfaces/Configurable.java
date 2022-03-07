package hbase.base.interfaces;

import java.util.Collection;
import java.util.Properties;

import static java.util.Collections.emptySet;

/**
 * Generic interface for configurable instances
 */
public interface Configurable {
    /**
     * Returns a set with all properties this object accepts. By default, an empty set
     * <p>
     * The following rules are considered:
     *
     * <li>configs ending with a dot "." are considered to be a prefix group;</li>
     * <li>configs ending with an interrogation "?" are considered as nullable.</li>
     **/
    default Collection<String> configs() {
        return emptySet();
    }

    /**
     * Sets up a non-nullable configuration.
     * <p>
     * The config value returned by {@link Config#getValue(String)} MUST not be non-null
     *
     * @param name   name of the config. Required to not have a trailing {@code .} or {@code ?} in {@link #configs()}
     * @param config config object
     * @throws ClassCastException config value is non-null, but has the wrong type
     */
    default void configure(String name, Config config) {
        // nothing to do by default
    }

    /**
     * Sets up a nullable configuration, i.e., one listed with a {@code ?} in {@link #configs()}
     * <p>
     * <li>The config value returned by {@link Config#getValue(String)} can be null;</li>
     * <li>The trailing {@code ?} must be absent in {@code name}.</li>
     *
     * @param name   name of the config without the trailing {@code ?}
     * @param config config object
     * @throws ClassCastException config value is non-null, but has the wrong type
     */
    default void configureNullable(String name, Config config) {
        // nothing to do by default
    }

    /**
     * Sets up a configuration prefix, i.e., one listed with a {@code .} in {@link #configs()}
     *
     * @param prefix config prefix with the trailing {@code .}
     * @param props  properties corresponding to the given prefix
     */
    default void configure(String prefix, Properties props) {
        // nothing to do by default
    }

}
