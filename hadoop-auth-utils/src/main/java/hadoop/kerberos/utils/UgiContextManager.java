package hadoop.kerberos.utils;

import hbase.base.exceptions.UncheckedInterruptionException;
import hbase.base.interfaces.IOFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Executes code within a logged-in UGI context
 */
public class UgiContextManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(UgiContextManager.class);
    private static boolean kerberosEnabled = false;

    public static final String HADOOP_AUTH = "hadoop.security.authentication";

    private UgiContextManager() {
        // utility class
    }

    public synchronized static void enableKerberos() {
        if (!kerberosEnabled) {
            Configuration conf = new Configuration();
            conf.set(HADOOP_AUTH, "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            kerberosEnabled = true;
            LOGGER.info("Enabled Kerberos authentication for Hadoop");
        }
    }

    public static <T> T enterDefault(IOFunction<UserGroupInformation, T> code) throws IOException {
        LOGGER.info("Entering with default user");
        return enterWithUser(UserGroupInformation.getCurrentUser(), code);
    }

    public static <T> T enterWithKeytab(String principal, String keyTab, IOFunction<UserGroupInformation, T> code) throws IOException {
        LOGGER.info("Logging with principal={} and keyTab={}", principal, keyTab);
        UserGroupInformation.loginUserFromKeytab(principal, keyTab);
        return enterWithUser(UserGroupInformation.getCurrentUser(), code);
    }

    public static <T> T enterWithUser(UserGroupInformation user, IOFunction<UserGroupInformation, T> code) throws IOException {
        try {
            LOGGER.info("Entering privileged context for user={}", user);
            return user.doAs((PrivilegedExceptionAction<? extends T>) () -> {
                UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
                LOGGER.info("Executing privileged action with currentUser={}", currentUser);
                try {
                    return code.apply(currentUser);
                } finally {
                    LOGGER.info("Finished privileged action for currentUser={}", currentUser);
                }
            });
        } catch (InterruptedException e) {
            LOGGER.warn("Thread interrupted while executing code within UGI context");
            Thread.currentThread().interrupt();
            throw new UncheckedInterruptionException(e);
        }
    }

}
