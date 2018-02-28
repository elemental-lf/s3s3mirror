package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.SSECustomerKey;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class MirrorContext {

    @Getter @Setter private MirrorOptions options;
    @Getter @Setter private AmazonS3 sourceClient;
    @Getter @Setter private AmazonS3 destinationClient;
    @Getter @Setter private SSECustomerKey sourceSSEKey;
    @Getter @Setter private SSECustomerKey destinationSSEKey;
    @Getter private final MirrorStats stats = new MirrorStats();
}
