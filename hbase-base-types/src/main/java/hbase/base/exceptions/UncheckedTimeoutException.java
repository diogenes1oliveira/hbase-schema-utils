package hbase.base.exceptions;

import java.util.concurrent.TimeoutException;

/**
 * Unchecked wrapper exception for {@link TimeoutException}
 */
public class UncheckedTimeoutException extends RuntimeException {
    public UncheckedTimeoutException(String message, TimeoutException e) {
        super(message, e);
    }

    public UncheckedTimeoutException(TimeoutException e) {
        super(e);
    }

    @Override
    public synchronized TimeoutException getCause() {
        return (TimeoutException) super.getCause();
    }
}
