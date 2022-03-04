package hbase.base.exceptions;

/**
 * Unchecked wrapper exception for {@link InterruptedException}
 */
public class UncheckedInterruptionException extends RuntimeException {
    public UncheckedInterruptionException(String message, InterruptedException e) {
        super(message, e);
    }

    public UncheckedInterruptionException(InterruptedException e) {
        super(e);
    }

    @Override
    public InterruptedException getCause() {
        return (InterruptedException) super.getCause();
    }
}
