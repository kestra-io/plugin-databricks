package io.kestra.plugin.databricks.dbfs;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/**
 * Guards the file.size metric fix used by Upload and Download.
 *
 * IOUtils.copy returns an int and reports -1 once the transferred count passes Integer.MAX_VALUE,
 * so both tasks use IOUtils.copyLarge, which returns a long. This feeds copyLarge a virtual stream
 * larger than Integer.MAX_VALUE without allocating or moving real bytes, so the boundary the fix
 * targets is exercised in CI instead of only above 2 GB against a live Databricks workspace.
 */
class LargeCopyTest {
    private static final long LENGTH = Integer.MAX_VALUE + 1_024L;

    @Test
    void copyLargeReportsByteCountAboveIntegerMax() throws IOException {
        try (InputStream in = new ZeroInputStream(LENGTH); OutputStream out = OutputStream.nullOutputStream()) {
            long copied = IOUtils.copyLarge(in, out);

            assertThat(copied, greaterThan((long) Integer.MAX_VALUE));
            assertThat(copied, is(LENGTH));
        }
    }

    /**
     * Yields a fixed number of zero bytes without allocating them, so a multi-gigabyte stream costs
     * only CPU. The bytes read into the caller's buffer are left untouched since a fresh buffer is
     * already zero-filled and copyLarge only forwards the reported length.
     */
    private static final class ZeroInputStream extends InputStream {
        private long remaining;

        ZeroInputStream(long length) {
            this.remaining = length;
        }

        @Override
        public int read() {
            if (remaining <= 0) {
                return -1;
            }
            remaining--;
            return 0;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (remaining <= 0) {
                return -1;
            }
            int n = (int) Math.min(len, remaining);
            remaining -= n;
            return n;
        }
    }
}
