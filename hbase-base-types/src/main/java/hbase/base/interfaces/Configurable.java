package hbase.base.interfaces;

/**
 * Generic interface for configurable instances
 */
public interface Configurable {
    /**
     * Configures this object
     * <p>
     * The default implementation does nothing
     *
     * @param config config object
     */
    default void configure(Config config) {
        // nothing to do by default
    }

}
