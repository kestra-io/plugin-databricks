package io.kestra.plugin.databricks.utils;

public final class LogSanitizer {
    private LogSanitizer() {
        //utility class pattern
    }

    /**
     * Strips control characters from content that ends up verbatim in the worker logs (e.g. a user-supplied
     * seed, or a Databricks task's stdout/stderr) so it cannot forge or break the log stream.
     */
    public static String stripControlChars(String text) {
        return text.replaceAll("\\p{Cntrl}", " ");
    }
}
