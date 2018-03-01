package org.cobbzilla.s3s3mirror;

import com.amazonaws.auth.AWSCredentials;
import lombok.Getter;
import lombok.Setter;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.spec.KeySpec;

public class MirrorProfile implements AWSCredentials {

    // These are for AWSCredentials
    @Getter @Setter private String aWSAccessKeyId;
    @Getter @Setter private String aWSSecretKey;

    // Our fields
    @Getter @Setter private String name;
    @Getter @Setter private String endpoint;

    @Getter @Setter public String proxyHost = null;
    @Getter @Setter public int proxyPort = -1;

    @Getter private MirrorEncryption encryption = MirrorEncryption.NONE;
    @Getter private SecretKey encryptionKey = null;

    public void setEncryption(String encryptionName) {
        try {
            this.encryption = MirrorEncryption.valueOf(encryptionName);
        } catch (Exception e) {
            throw new IllegalStateException("Unknown encryption method: " + encryptionName);
        }
    }

    public void setEncryptionKey(String passphrase) {
        // This isn't ideal. But I don't see a good way to save a randomly generated salt somewhere.
        final byte[] salt = {(byte) 154, (byte) 146, (byte) 100, (byte) 145, (byte) 154, (byte) 145, (byte) 155,
                (byte) 145, (byte) 156, (byte) 164, (byte) 141, (byte) 154, (byte) 056, (byte) 156, (byte) 145, (byte) 164};

        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            KeySpec spec = new PBEKeySpec(passphrase.toCharArray(), salt, 65536, 256);
            SecretKey derivedKey = factory.generateSecret(spec);
            encryptionKey = new SecretKeySpec(derivedKey.getEncoded(), "AES");
        } catch (Exception e) {
            throw new IllegalStateException("Could not derive key", e);
        }
    }

    public boolean getHasProxy() {
        boolean hasProxyHost = proxyHost != null && proxyHost.trim().length() > 0;
        boolean hasProxyPort = proxyPort != -1;

        return hasProxyHost && hasProxyPort;
    }

    public boolean isValid() {
        // These must be set
        boolean valid = name != null && aWSAccessKeyId != null && aWSSecretKey != null && endpoint != null;

        switch (encryption) {
            case NONE:
            case SSE_S3:
            case SSE_KMS_DFK:
                break;
            case CSE_AES_256:
            case CSE_AES_GCM_256:
            case CSE_AES_GCM_256_STRICT:
            case SSE_C:
                valid = valid && encryptionKey != null;
                break;
            default:
                // Shouldn't happen...
                valid = false;
        }

        return valid;
    }

    public boolean equals(MirrorProfile otherProfile) {
        return this.getName().equals(otherProfile.getName());
    }
}
