package org.cobbzilla.s3s3mirror;

import lombok.Getter;
import lombok.SneakyThrows;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.zip.Checksum;

public class FingerprintedInputStream extends FilterInputStream {
    @Getter private final String fingerprintAlgorithm = "SHA-256";
    private MessageDigest digester = null;

    private Checksum cksum;

    @SneakyThrows // Should not throw anything under normal circumstances
    public FingerprintedInputStream(InputStream in) {
        super(in);
        digester = MessageDigest.getInstance(fingerprintAlgorithm);
    }

    public int read() throws IOException {
        int b = this.in.read();
        if (b != -1) {
            digester.update((byte) b);
        }

        return b;
    }

    public int read(byte[] buf, int off, int len) throws IOException {
        len = this.in.read(buf, off, len);
        if (len != -1) {
            digester.update(buf, off, len);
        }

        return len;
    }

    public long skip(long n) throws IOException {
        byte[] buf = new byte[8192];

        long total;
        long len;
        for(total = 0L; total < n; total += len) {
            len = n - total;
            len = (long)this.read(buf, 0, len < (long)buf.length ? (int)len : buf.length);
            if (len == -1L) {
                return total;
            }
        }

        return total;
    }

    public String getFingerprint() {
        return Base64.getEncoder().encodeToString(digester.digest());
    }
}
