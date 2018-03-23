package org.cobbzilla.s3s3mirror;

import org.junit.Test;

import javax.crypto.SecretKey;

import static org.junit.Assert.assertArrayEquals;

public class MirrorProfileTest {

    @Test
    public void testDeriveKey () throws Exception {
        final String key = "test";
        /*
           This derived key was generated with a Python implementation of the same algorithm:

           # Requires cryptodome
           from Crypto.Protocol.KDF import PBKDF2
           from Crypto.Hash import HMAC
           from Crypto.Hash import SHA256

           password = "test"
           salt = bytes([154, 146, 100, 145, 154, 145, 155, 145, 156, 164, 141, 154, 46, 156, 145, 164])
           derived_key = PBKDF2(password, salt, 32, 65536, prf=lambda p,s : HMAC.new(p,s, SHA256).digest())
         */
        final byte[] expectedDerviedKey = {(byte)  11, (byte) 255, (byte)  51, (byte) 123,
                                           (byte) 138, (byte)  70, (byte) 156, (byte) 220,
                                           (byte) 237, (byte)  58, (byte)   8, (byte)  11,
                                           (byte) 162, (byte)  37, (byte) 152, (byte)  41,
                                           (byte)  47, (byte)  97, (byte) 221, (byte) 235,
                                           (byte) 136, (byte) 220, (byte) 128, (byte) 130,
                                           (byte)  17, (byte)  14, (byte) 145, (byte)  78,
                                           (byte)  39, (byte)  71, (byte)  11, (byte) 225};

        SecretKey derivedKey = MirrorProfile.deriveKey(key);

        assertArrayEquals(expectedDerviedKey, derivedKey.getEncoded());
    }
}
