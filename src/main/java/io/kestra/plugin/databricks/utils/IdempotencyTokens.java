package io.kestra.plugin.databricks.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class IdempotencyTokens {
    private IdempotencyTokens() {
    }

    /**
     * Derives a deterministic Databricks idempotency token for a given seed and generation.
     * Same seed and generation always produce the same token; a SHA-256 hex digest is always
     * exactly 64 characters, which is the maximum length Databricks accepts.
     */
    public static String token(String seed, int generation) {
        try {
            var digest = MessageDigest.getInstance("SHA-256")
                .digest((seed + ":" + generation).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed to be available on every JVM implementation
            throw new IllegalStateException(e);
        }
    }
}
