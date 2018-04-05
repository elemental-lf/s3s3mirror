package org.cobbzilla.s3s3mirror;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import lombok.Getter;
import lombok.Setter;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.spec.KeySpec;
import java.util.ArrayList;

public class MirrorProfile implements AWSCredentials {

    // These are for AWSCredentials
    @Getter @Setter private String aWSAccessKeyId;
    @Getter @Setter private String aWSSecretKey;
    @Getter @Setter private String signerType;
    @Getter @Setter private String region = Regions.US_EAST_1.name();

    // Our fields
    @Getter @Setter private String name;
    @Getter @Setter private String endpoint;

    @Getter @Setter public String proxyHost = null;
    @Getter @Setter public int proxyPort = -1;

    @Getter private MirrorEncryption encryption = MirrorEncryption.NONE;
    @Getter private SecretKey encryptionKey = null;

    @Getter private ArrayList<MirrorProfileQuirks> quirks = new ArrayList<MirrorProfileQuirks>();

    public void setEncryption(String encryptionName) {
        try {
            this.encryption = MirrorEncryption.valueOf(encryptionName);
        } catch (Exception e) {
            throw new IllegalStateException("Unknown encryption method: " + encryptionName);
        }
    }

    public static SecretKey deriveKey(String passphrase) {
        // This isn't ideal. But I don't see a good way to save a randomly generated salt somewhere.
        final byte[] salt = {(byte) 154, (byte) 146, (byte) 100, (byte) 145, (byte) 154, (byte) 145, (byte) 155,
                (byte) 145, (byte) 156, (byte) 164, (byte) 141, (byte) 154, (byte) 46, (byte) 156, (byte) 145, (byte) 164};

        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            KeySpec spec = new PBEKeySpec(passphrase.toCharArray(), salt, 65536, 256);
            SecretKey derivedKey = factory.generateSecret(spec);
            return new SecretKeySpec(derivedKey.getEncoded(), "AES");
        } catch (Exception e) {
            throw new IllegalStateException("Could not derive key", e);
        }
    }

    public void setEncryptionKey(String passphrase) {
        encryptionKey = deriveKey(passphrase);
    }

    public boolean hasProxy() {
        boolean hasProxyHost = proxyHost != null && proxyHost.trim().length() > 0;
        boolean hasProxyPort = proxyPort != -1;

        return hasProxyHost && hasProxyPort;
    }

    public boolean isValid() {
        // These must be set
        boolean valid = name != null && aWSAccessKeyId != null && aWSSecretKey != null && endpoint != null;

        switch (encryption) {
            case NONE:
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

    public void addQuirk(MirrorProfileQuirks quirk) {
        quirks.add(quirk);
    }

    public boolean hasQuirk(MirrorProfileQuirks quirk) {
        return quirks.contains(quirk);
    }

    public boolean equals(MirrorProfile otherProfile) {
        return this.getName().equals(otherProfile.getName());
    }
}
