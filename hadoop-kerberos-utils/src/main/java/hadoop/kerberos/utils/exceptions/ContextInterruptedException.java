package hadoop.kerberos.utils.exceptions;

/**
 * Encountered {@link InterruptedException} while executing code within the Kerberos context
 */
public class ContextInterruptedException extends IllegalStateException {
    public ContextInterruptedException(InterruptedException cause) {
        super(cause);
    }

    @Override
    public synchronized InterruptedException getCause() {
        return (InterruptedException) super.getCause();
    }
}
