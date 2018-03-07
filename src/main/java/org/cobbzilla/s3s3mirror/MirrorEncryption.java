package org.cobbzilla.s3s3mirror;

import java.util.EnumSet;

public enum MirrorEncryption {
    NONE, SSE_C, CSE_AES_256, CSE_AES_GCM_256, CSE_AES_GCM_256_STRICT;

    protected static EnumSet<MirrorEncryption> getCSEAlgorithms() {
        return EnumSet.of(CSE_AES_256, CSE_AES_GCM_256, CSE_AES_GCM_256_STRICT);
    }

    protected static boolean isCSE(MirrorEncryption encryption) {
        return getCSEAlgorithms().contains(encryption);
    }

    protected static EnumSet<MirrorEncryption> getSSEAlgorithms() {
        return EnumSet.of(SSE_C);
    }

    protected static boolean isSSE(MirrorEncryption encryption) {
        return getSSEAlgorithms().contains(encryption);
    }

    /*
        SSE_C:

        Same as above but with user supplied key. Again other implementations might do something different here. For
        example minio either uses AES-256/GCM or ChaCah20/Poly1305 depending on the platform. Also with envelope
        encryption. So please check when you're using server-side data at rest encryption, what your endpoint implements.

        CSE_AES_256:

        Implemented in the S3 client. Uses AES-256 with envelope encryption.

        CSE_AES_GCM_256:

        Implemented in the S3 client. Uses AES-256/GCM with envelope encryption. Retrieved objects may be unauthenticated
        but still need to be encrypted, i.e. objects encrypted with CSE_AES_256 are also allowed. This mode is useful
        when transitioning from CSE_AES_256 to CSE_AES_GCM_256_STRICT.

        CSE_AES_GCM_256_STRICT:

        Same as CSE_AES_GCM_256 but all retrieved objects must use authenticated encryption.
     */
}
