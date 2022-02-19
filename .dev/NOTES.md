Kerberos Context as in:

```java
UserGroupInformation.setConfiguration(kerberosConf);
UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTab);
try {
    return ugi.doAs((PrivilegedExceptionAction<T>) action::get);
} catch (InterruptedException e) {
    LOGGER.warn("Interrupted while executing code within Kerberos context", e);
    Thread.currentThread().interrupt();
    throw new IOException("Interrupted while executing code within Kerberos context", e);
}

```
