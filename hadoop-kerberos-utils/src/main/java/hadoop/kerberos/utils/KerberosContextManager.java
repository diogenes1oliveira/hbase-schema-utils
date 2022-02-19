package hadoop.kerberos.utils;

import hadoop.kerberos.utils.interfaces.AuthContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Executes code within a logged-in UGI context;
 */
public class KerberosContextManager {
    public AuthContext<UserGroupInformation, IOException> enterDefault(Configuration conf) throws IOException {
        return new UgiConfigContext(conf, conf, UserGroupInformation::getCurrentUser);
    }

    public AuthContext<UserGroupInformation, IOException> enterWithKeytab(Configuration conf,
                                                                          String principal,
                                                                          String keytab) throws IOException {
        Configuration contextConf = new Configuration(conf);
        setKerberosConf(contextConf);

        return new UgiConfigContext(conf, contextConf, () ->
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
        );
    }

    protected void setKerberosConf(Configuration conf) {
        conf.set("hadoop.security.authentication", "Kerberos");
    }
}
