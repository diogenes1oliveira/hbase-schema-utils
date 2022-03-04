package hadoop.kerberos.utils.interfaces;


import hbase.base.exceptions.UncheckedInterruptionException;

import java.util.function.Supplier;

/**
 * Interface compatible with try-with-resources to execute actions within a privileged context in
 * a thread-safe manner.
 *
 * @param <C> context object type
 * @param <E> checked exception thrown by authentication context
 */
public interface AuthContext<C, E extends Exception> extends AutoCloseable {
    /**
     * Executes a runnable within the Kerberos context
     * <p>
     * The default implementation just forwards to {@link #get(Supplier)}
     *
     * @param runnable code to execute within the Kerberos context
     * @throws UncheckedInterruptionException thread interrupted while executing the code. Before throwing this
     *                                        exception, implementations must log this error and call
     *                                        {@code Thread.currentThread().interrupt()}, in this order.
     * @throws RuntimeException               runnable failed
     */
    default void run(Runnable runnable) throws E {
        get(() -> {
            runnable.run();
            return null;
        });
    }

    /**
     * The object containing the authentication data
     */
    C context();

    /**
     * @param supplier code to supply a value within the Kerberos context
     * @param <T>      supplied value type
     * @throws UncheckedInterruptionException thread interrupted while executing the code. Before throwing this
     *                                        exception, implementations must log this error and call
     *                                        {@code Thread.currentThread().interrupt()} in this order
     * @throws RuntimeException               supplier failed
     */
    <T> T get(Supplier<T> supplier) throws E;

    /**
     * Cleans up the context
     */
    void close() throws E;
}
