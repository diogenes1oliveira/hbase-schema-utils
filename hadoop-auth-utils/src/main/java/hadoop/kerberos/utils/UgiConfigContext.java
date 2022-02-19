package hadoop.kerberos.utils;

import hadoop.kerberos.utils.exceptions.ContextInterruptedException;
import hadoop.kerberos.utils.interfaces.IOAuthContext;
import hadoop.kerberos.utils.interfaces.IOSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

/**
 * Authentication context that sets and restores the static UGI information
 * <p>
 * Because this requires a global lock, you should take care of only calling this context
 * when necessary (for instance, when recreating a connection)
 */
public class UgiConfigContext implements IOAuthContext<UserGroupInformation> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UgiConfigContext.class);

    private final Configuration originalConf;
    private final Lock lock;
    private final UserGroupInformation ugi;

    /**
     * @param originalConf    Hadoop's configuration to be restored outside the context
     * @param contextConf     Hadoop's configuration inside the context
     * @param lock            lock object to be acquired before any changes to the UGI
     * @param localUgiFactory object to supply the UGI inside the context
     * @throws IOException failed to create UGI
     */
    public UgiConfigContext(Configuration originalConf,
                            Configuration contextConf,
                            Lock lock,
                            IOSupplier<UserGroupInformation> localUgiFactory) throws IOException {
        this.originalConf = originalConf;
        this.lock = lock;
        this.lock.lock();

        UserGroupInformation.setConfiguration(contextConf);
        this.ugi = localUgiFactory.get();
    }

    /**
     * Returns the {@link UserGroupInformation} instance within the context
     */
    @Override
    public UserGroupInformation context() {
        return ugi;
    }

    /**
     * Calls the supplier within the UGI context
     *
     * @param supplier code to supply a value within the Kerberos context
     * @param <T>      type of supplied value
     * @return value generated by supplier within authentication context
     * @throws IOException exception thrown by {@link UserGroupInformation#doAs(PrivilegedExceptionAction)}
     */
    @Override
    public <T> T get(Supplier<T> supplier) throws IOException {
        try {
            return ugi.doAs((PrivilegedExceptionAction<? extends T>) supplier::get);
        } catch (InterruptedException e) {
            LOGGER.warn("Thread interrupted while executing code within UGI context");
            Thread.currentThread().interrupt();
            throw new ContextInterruptedException(e);
        }
    }

    /**
     * Restores the original UGI information and releases the global lock
     */
    @Override
    public void close() {
        UserGroupInformation.setConfiguration(originalConf);
        lock.unlock();
    }
}
