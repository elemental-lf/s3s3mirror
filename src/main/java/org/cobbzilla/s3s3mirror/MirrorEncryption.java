package org.cobbzilla.s3s3mirror;

import java.util.EnumSet;

public enum MirrorEncryption {
    NONE, SSE_S3, SSE_C, SSE_KMS_DFK, CSE_AES_256, CSE_AES_GCM_256, CSE_AES_GCM_256_STRICT;

    protected static EnumSet<MirrorEncryption> getCSEAlgorithms() {
        return EnumSet.of(CSE_AES_256, CSE_AES_GCM_256, CSE_AES_GCM_256_STRICT);
    }

    protected static boolean isCSE(MirrorEncryption encryption) {
        return getCSEAlgorithms().contains(encryption);
    }

    /*
        SSE_S3:

        With Amazon this is AES-256 and envelope encryption. Other S3 implementations might, use other algorithms. For
        example minio either uses AES-256/GCM or ChaCah20/Poly1305 depending on the platform (also with envelope
        encryption). So please check when you're using server-side data at rest encryption, what your
        endpoint implements.

        SSE_C:

        Same as above but with user supplied key. Again other implementations might do something different here. For
        example minio either uses AES-256/GCM or ChaCah20/Poly1305 depending on the platform. Also with envelope
        encryption. So please check when you're using server-side data at rest encryption, what your endpoint implements.

        SSE_KMS_DFK:

        Only works with Amazon to my knowledge (Amazon speak: SSE-KMS). Key management done via AWS KMS API.
        This uses the default master key.

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
