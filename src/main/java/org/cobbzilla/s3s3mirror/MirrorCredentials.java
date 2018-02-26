package org.cobbzilla.s3s3mirror;

import com.amazonaws.auth.AWSCredentials;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@EqualsAndHashCode
public class MirrorCredentials implements AWSCredentials {

    @Getter @Setter private String aWSAccessKeyId;
    @Getter @Setter private String aWSSecretKey;
    @Getter @Setter private String endpoint;

    public boolean hasAwsKeys() { return aWSAccessKeyId != null && aWSSecretKey != null; }
    public boolean hasEndpoint() { return endpoint != null; }
}
