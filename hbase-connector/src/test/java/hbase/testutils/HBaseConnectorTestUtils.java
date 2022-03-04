package hbase.testutils;

import hbase.base.exceptions.UncheckedInterruptionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Helper methods
 */
public final class HBaseConnectorTestUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConnectorTestUtils.class);

    private HBaseConnectorTestUtils() {
        // utility class
    }

    /**
     * Executes a separate thread, waiting for it to actually be up
     *
     * @param code runnable to be executed in a thread
     * @return started thread
     * @throws UncheckedInterruptionException interrupted while waiting for thread
     */
    public static Thread startThread(ThrowingRunnable<Exception> code) {
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            latch.countDown();
            try {
                code.run();
            } catch (Exception e) {
                LOGGER.error("error in thread", e);
                throw new RuntimeException(e);
            }
        });
        thread.start();
        try {
            if (!latch.await(1, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Thread took too long to start");
            }
            Thread.sleep(100);
        } catch (InterruptedException e) {
            LOGGER.error("interrupted while waiting for thread to start", e);
            Thread.currentThread().interrupt();
            thread.interrupt();
            throw new UncheckedInterruptionException("interrupted while waiting for thread to start", e);
        }

        return thread;
    }
}
