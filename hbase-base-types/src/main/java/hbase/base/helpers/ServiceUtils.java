package hbase.base.helpers;

/**
 * Helpers to deal with Services
 */
public class ServiceUtils {
    private ServiceUtils() {
        // utility class
    }

    /**
     * Returns {@code true} if the class is from this repo
     */
    public static boolean isFromThisRepo(Class<?> clazz) {
        return clazz.getPackage().getName().startsWith("hbase.");
    }
}
