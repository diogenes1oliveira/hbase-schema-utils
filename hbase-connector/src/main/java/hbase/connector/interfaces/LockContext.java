package hbase.connector.interfaces;

/**
 * Provides an interface compatible with try-with-resources that unlocks a synchronization lock
 * on closing
 */
@FunctionalInterface
public interface LockContext extends AutoCloseable {
    void unlock();

    /**
     * This default implementation just forwards to {@link #unlock()}.
     * <p>
     * (you probably won't need to change this)
     */
    @Override
    default void close() {
        unlock();
    }
}
