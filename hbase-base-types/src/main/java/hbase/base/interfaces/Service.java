package hbase.base.interfaces;

import static hbase.base.helpers.ServiceUtils.isFromThisRepo;

/**
 * Generic interface for loadable Services
 */
public interface Service {
    /**
     * Returns a priority number in case two factories support the configuration
     * <p>
     * The default implementation returns 0 for implementations from this repository and 100 otherwise
     *
     * @return a number for the priority
     */
    default int priority() {
        return isFromThisRepo(getClass()) ? 0 : 100;
    }
}
