package hadoop.kerberos.utils.interfaces;

import java.io.IOException;

/**
 * Specialization to {@link AuthContext} that throws {@link IOException} exceptions
 *
 * @param <C> context type
 */
public interface IOAuthContext<C> extends AuthContext<C, IOException> {
    // helper interface
}
