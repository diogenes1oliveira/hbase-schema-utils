package hadoop.kerberos.utils;

import hadoop.kerberos.utils.interfaces.IOAuthContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Executes code within a logged-in UGI context
 */
public class UgiGlobalContextManager {
    private static final Lock globalLock = new ReentrantLock();

    private UgiGlobalContextManager() {
        // utility class
    }

    public static IOAuthContext<UserGroupInformation> enterDefault() throws IOException {
        Configuration originalConf = new Configuration();
        return new UgiConfigContext(originalConf, originalConf, globalLock, UserGroupInformation::getCurrentUser);
    }

    public static IOAuthContext<UserGroupInformation> enterWithKeytab(String principal,
                                                                      String keytab) throws IOException {
        Configuration originalConf = new Configuration();
        Configuration kerberosConf = new Configuration();
        kerberosConf.set("hadoop.security.authentication", "Kerberos");

        return new UgiConfigContext(originalConf, kerberosConf, globalLock, () ->
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
        );
    }

}
