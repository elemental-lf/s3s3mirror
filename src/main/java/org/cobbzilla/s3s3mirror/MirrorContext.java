package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class MirrorContext {

    @Getter @Setter private MirrorOptions options;
    @Getter @Setter private AmazonS3Client sourceClient;
    @Getter @Setter private AmazonS3Client destinationClient;
    @Getter private final MirrorStats stats = new MirrorStats();
}
