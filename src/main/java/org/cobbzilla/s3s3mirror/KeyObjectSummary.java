package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeyObjectSummary implements Serializable {

    @Getter @Setter private String bucketName;
    @Getter @Setter private String key;
    @Getter @Setter private String eTag;
    @Getter @Setter private long size;
    @Getter @Setter private Date lastModified;
    @Getter @Setter private String storageClass;
    @Getter @Setter private Owner owner;
    @Getter @Setter private String versionId;
    @Getter @Setter private boolean deleteMarker = false;
    @Getter @Setter private boolean latest = true;

    private static Function<S3ObjectSummary, KeyObjectSummary> S3ObjectSummaryToKeyObjectSummaryFunction
            = new Function<S3ObjectSummary, KeyObjectSummary>() {

        public KeyObjectSummary apply(S3ObjectSummary input) {
            KeyObjectSummary output = new KeyObjectSummary();

            output.setBucketName(input.getBucketName());
            output.setKey(input.getKey());
            output.setETag(input.getETag());
            output.setSize(input.getSize());
            output.setLastModified(input.getLastModified());
            output.setStorageClass(input.getStorageClass());
            output.setOwner(input.getOwner());

            return output;
        }
    };

    private static Function<S3VersionSummary, KeyObjectSummary> S3VersionSummaryToKeyObjectSummaryFunction
            = new Function<S3VersionSummary, KeyObjectSummary>() {

        public KeyObjectSummary apply(S3VersionSummary input) {
            KeyObjectSummary output = new KeyObjectSummary();

            output.setBucketName(input.getBucketName());
            output.setKey(input.getKey());
            output.setETag(input.getETag());
            output.setSize(input.getSize());
            output.setLastModified(input.getLastModified());
            output.setStorageClass(input.getStorageClass());
            output.setOwner(input.getOwner());
            output.setVersionId(input.getVersionId());
            output.setDeleteMarker(input.isDeleteMarker());
            output.setLatest(input.isLatest());

            return output;
        }
    };

    public static List<KeyObjectSummary> S3ObjectSummaryToKeyObject(List<S3ObjectSummary> input) {
        return input.stream().map(S3ObjectSummaryToKeyObjectSummaryFunction).collect(Collectors.<KeyObjectSummary>toList());
    }

    public static List<KeyObjectSummary> S3VersionSummaryToKeyObject(List<S3VersionSummary> input) {
        return input.stream().map(S3VersionSummaryToKeyObjectSummaryFunction).collect(Collectors.<KeyObjectSummary>toList());
    }

    @Override
    public String toString() {
        return "KeyObjectSummary{" +
                "bucketName='" + bucketName + '\'' +
                ", key='" + key + '\'' +
                ", eTag='" + eTag + '\'' +
                ", size=" + size +
                ", lastModified=" + lastModified +
                ", storageClass='" + storageClass + '\'' +
                ", owner=" + owner +
                '}';
    }
}
