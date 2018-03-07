package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor @ToString
class S3Asset {
    @Getter private AmazonS3 client;
    @Getter private String bucket;
    @Getter private String key;
}
