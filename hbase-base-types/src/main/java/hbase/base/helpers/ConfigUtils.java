package hbase.base.helpers;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public final class ConfigUtils {
    private static final Pattern ENV_PATTERN = Pattern.compile("^[A-Z_][A-Z0-9_]*$");

    public static String normalizeConfigName(String name) {
        if (!ENV_PATTERN.matcher(name).matches()) {
            return name;
        }
        String[] escapedParts = name.split("___");
        List<String> parts = new ArrayList<>();
        parts.add(replaceUnderscores(escapedParts[0]));

        for (int i = 1; i < escapedParts.length; ++i) {
            String escapedPart = escapedParts[i];
            String part = escapedPart.charAt(0) + replaceUnderscores(escapedPart.substring(1));
            parts.add(part);
        }

        return String.join("", parts);
    }

    private static String replaceUnderscores(String s) {
        return s.replace("__", "-").replace("_", ".").toLowerCase(Locale.ROOT);
    }
}
