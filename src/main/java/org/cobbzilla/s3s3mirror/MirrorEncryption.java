package org.cobbzilla.s3s3mirror;

public enum MirrorEncryption {
    NONE, SSE_AES_256, SSE_C_AES_256, CSE_AES_GCM_256, CSE_AES_GCM_256_STRICT;
}
