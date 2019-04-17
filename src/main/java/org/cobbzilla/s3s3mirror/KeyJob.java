package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.*;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public abstract class KeyJob implements Runnable {

    final String USER_METADATA_CLEANUP_REGEXP = "(?i:^X-Amz-.*$)";

    protected final MirrorContext context;
    protected final KeyObjectSummary summary;
    protected final Object notifyLock;

    public KeyJob(MirrorContext context, KeyObjectSummary summary, Object notifyLock) {
        this.context = context;
        this.summary = summary;
        this.notifyLock = notifyLock;
    }

    public abstract Logger getLog();

    @Override public String toString() { return summary.getKey(); }

    private ObjectMetadata getObjectMetadata(AmazonS3 client, SSECustomerKey sseKey, String bucket, String key) throws Exception {
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();

        AmazonS3Exception lastException = null;
        ObjectMetadata metadata = null;
        for (int tries = 1; tries <= options.getMaxRetries(); tries++) {
            try {
                GetObjectMetadataRequest getRequest = new GetObjectMetadataRequest(bucket, key);

                setupSSEEncryption(getRequest, sseKey);

                context.getStats().s3getCount.incrementAndGet();
                metadata = client.getObjectMetadata(getRequest);
                break;

            } catch (AmazonS3Exception e) {
                lastException = e;
                if (e.getStatusCode() == 404) {
                    // Key not found
                    break;
                } else {
                    // Ignore and try again
                    if (verbose) log.warn("GetObjectMetadataRequest for {} failed (try #{}).", key, tries, e);
                }
            }

            if (Sleep.sleep(10)) break;
        }

        if (metadata != null) {
            return metadata;
        } else if (lastException.getStatusCode() == 404) {
            throw lastException;
        } else {
            log.error("getObjectMetadata for {} failed after {} tries, giving up.", key, options.getMaxRetries(), lastException);
            throw lastException;
        }
    }

    protected ObjectMetadata getSourceObjectMetadata(String key) throws Exception {
    	return getObjectMetadata(context.getSourceClient(), context.getSourceSSEKey(),
                context.getOptions().getSourceBucket(), key);
    }

    protected ObjectMetadata getDestinationObjectMetadata(String key) throws Exception {
    	return getObjectMetadata(context.getDestinationClient(), context.getDestinationSSEKey(),
                context.getOptions().getDestinationBucket(), key);
    }

    private AccessControlList getAccessControlList(AmazonS3 client, SSECustomerKey sseKey, String bucket, String key) throws Exception {
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();

        AccessControlList acl = null;
        for (int tries = 1; tries <= options.getMaxRetries(); tries++) {
            try {
                GetObjectAclRequest getObject = new GetObjectAclRequest(bucket, key);

                context.getStats().s3getCount.incrementAndGet();
                acl = client.getObjectAcl(getObject);
                break;
            } catch (AmazonS3Exception se3) {
                // Ignore and try again
                if (verbose) log.warn("GetObjectAclRequest for {} failed (try #{})", key, tries);
            }

            if (Sleep.sleep(10)) break;
        }

        if (acl != null) {
            return acl;
        } else {
            // Annoyingly there can be two reasons for this to fail. It will fail if the IAM account
            // permissions are wrong, but it will also fail if we are copying an item that we don't
            // own ourselves. This may seem unusual, but it occurs when copying AWS Detailed Billing
            // objects since although they live in your bucket, the object owner is AWS.
            log.warn("Unable to obtain object ACL for {}, copying object without ACL data.", key);
            return new AccessControlList();
        }

    }

    protected AccessControlList getSourceAccessControlList(String key) throws Exception {
    	return this.getAccessControlList(context.getSourceClient(), context.getSourceSSEKey(), context.getOptions().getSourceBucket(), key);
    }

    @SneakyThrows
    protected static void logMetadata(String label, ObjectMetadata metadata) {
        Map userMetadataMap = metadata.getUserMetadata();
        Map rawMetadataMap = metadata.getRawMetadata();

        // Based on https://stackoverflow.com/questions/1760654/java-printstream-to-string
        final Charset charset = StandardCharsets.UTF_8;
        @Cleanup ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @Cleanup PrintStream ps = new PrintStream(baos, true, charset.name());

        MapUtils.debugPrint(ps, label + " user metadata", userMetadataMap);
        MapUtils.debugPrint(ps, label + " raw metadata", rawMetadataMap);

        String metadataString = new String(baos.toByteArray(), charset);

        log.info(metadataString);
    }

    protected ObjectMetadata buildDestinationMetadata(ObjectMetadata sourceMetadata) {
        ObjectMetadata destinationMetadata = new ObjectMetadata();

        if (sourceMetadata.getContentType() != null) destinationMetadata.setContentType(sourceMetadata.getContentType());
        if (sourceMetadata.getCacheControl() != null) destinationMetadata.setCacheControl(sourceMetadata.getCacheControl());
        if (sourceMetadata.getContentEncoding() != null) destinationMetadata.setContentEncoding(sourceMetadata.getContentEncoding());
        if (sourceMetadata.getContentLanguage() != null) destinationMetadata.setContentLanguage(sourceMetadata.getContentLanguage());
        if (sourceMetadata.getContentDisposition() != null) destinationMetadata.setContentDisposition(sourceMetadata.getContentDisposition());
        if (sourceMetadata.getHttpExpiresDate() != null) destinationMetadata.setHttpExpiresDate(sourceMetadata.getHttpExpiresDate());

        MirrorEncryption destinationEncryption = context.getOptions().getDestinationProfile().getEncryption();

        HashMap<String, String> userMetadataMap = new HashMap<String,String>();
        String length = null;

        for (Map.Entry<String,String> entry: sourceMetadata.getUserMetadata().entrySet()) {
            if (length == null && entry.getKey().toLowerCase().equals("x-amz-unencrypted-content-length")) {
                length = new String(entry.getValue());
            }

            if (!entry.getKey().matches(USER_METADATA_CLEANUP_REGEXP)) {
                userMetadataMap.put(entry.getKey(), entry.getValue());
            }
        }

        destinationMetadata.setUserMetadata(userMetadataMap);

        if (length != null) {
            if (context.getOptions().isVerbose())
                log.info("adjusting Content-Length from " + sourceMetadata.getContentLength() +
                        " to " + length);
            destinationMetadata.setContentLength(Long.parseLong(length));
        } else {
            destinationMetadata.setContentLength(sourceMetadata.getContentLength());
        }

        if (MirrorEncryption.isCSE(destinationEncryption)) {
            // The AWS SDK sometimes doesn't set this header -> always set it here as a workaround
            destinationMetadata.addUserMetadata(Headers.UNENCRYPTED_CONTENT_LENGTH, Long.toString(destinationMetadata.getContentLength()));
        } else if (MirrorEncryption.isSSE(destinationEncryption)) {
            destinationMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }

        return destinationMetadata;
    }

    protected static long getRealObjectSize(ObjectMetadata metadata) {
        String length = null;

        for (Map.Entry<String,String> entry: metadata.getUserMetadata().entrySet()) {
            if (entry.getKey().toLowerCase().equals("x-amz-unencrypted-content-length")) {
                length = new String(entry.getValue());
                break;
            }
        }

        return (length != null) ? Long.parseLong(length) : metadata.getContentLength();
    }

    protected void setupSSEEncryption(GetObjectRequest request, SSECustomerKey key) {
        if (key != null) request.setSSECustomerKey(key);
    }

    protected void setupSSEEncryption(GetObjectMetadataRequest request, SSECustomerKey key) {
        if (key != null) request.setSSECustomerKey(key);
    }

    protected void setupSSEEncryption(PutObjectRequest request, SSECustomerKey key) {
        if (key != null) request.setSSECustomerKey(key);
    }

    protected void setupSSEEncryption(CopyObjectRequest request, SSECustomerKey sourceKey, SSECustomerKey destinationKey) {
        if (sourceKey != null) request.setSourceSSECustomerKey(sourceKey);
        if (destinationKey != null) request.setDestinationSSECustomerKey(destinationKey);
    }

    protected void setupSSEEncryption(CopyPartRequest request, SSECustomerKey sourceKey, SSECustomerKey destinationKey) {
        if (sourceKey != null) request.setSourceSSECustomerKey(sourceKey);
        if (destinationKey != null) request.setDestinationSSECustomerKey(destinationKey);
    }

    protected void setupSSEEncryption(InitiateMultipartUploadRequest request, SSECustomerKey key) {
        if (key != null) request.setSSECustomerKey(key);
    }

    protected void setupSSEEncryption(UploadPartRequest request, SSECustomerKey key) {
        if (key != null) request.setSSECustomerKey(key);
    }
}
