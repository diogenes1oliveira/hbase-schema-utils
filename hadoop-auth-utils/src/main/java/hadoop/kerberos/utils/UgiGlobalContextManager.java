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

    public static IOAuthContext<UserGroupInformation> enterDefault(Configuration conf) throws IOException {
        return new UgiConfigContext(conf, conf, globalLock, UserGroupInformation::getCurrentUser);
    }

    public static IOAuthContext<UserGroupInformation> enterWithKeytab(Configuration conf,
                                                                      String principal,
                                                                      String keytab) throws IOException {
        Configuration confCopy = new Configuration(conf);
        setKerberosConf(conf);

        return new UgiConfigContext(confCopy, conf, globalLock, () ->
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
        );
    }

    private static void setKerberosConf(Configuration conf) {
        conf.set("hadoop.security.authentication", "Kerberos");
    }
}
