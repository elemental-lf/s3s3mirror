package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3;
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
import java.util.Iterator;
import java.util.Map;

@Slf4j
public abstract class KeyJob implements Runnable {

    final String USER_METADATA_CLEANUP_REGEXP = "(?i:^X-Amz-.*$)";

    protected final MirrorContext context;
    protected final S3ObjectSummary summary;
    protected final Object notifyLock;

    public KeyJob(MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        this.context = context;
        this.summary = summary;
        this.notifyLock = notifyLock;
    }

    public abstract Logger getLog();

    @Override public String toString() { return summary.getKey(); }

    private ObjectMetadata getObjectMetadata(AmazonS3 client, SSECustomerKey sseKey, String bucket, String key) throws Exception {
        MirrorOptions options = context.getOptions();
        Exception ex = null;
        for (int tries=0; tries < options.getMaxRetries(); tries++) {
            try {
                GetObjectMetadataRequest getRequest = new GetObjectMetadataRequest(bucket, key);

                setupSSEEncryption(getRequest, sseKey);

                context.getStats().s3getCount.incrementAndGet();
                return client.getObjectMetadata(getRequest);

            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404) throw e;

            } catch (Exception e) {
                ex = e;
                if (options.isVerbose()) {
                    if (tries >= options.getMaxRetries()) {
                        getLog().error("getObjectMetadata(" + key + ") failed (try #" + tries + "), giving up");
                        break;
                    } else {
                        getLog().warn("getObjectMetadata("+key+") failed (try #"+tries+"), retrying...");
                    }
                }
            }
        }
        throw ex;
    }
    
    protected ObjectMetadata getSourceObjectMetadata(String key) throws Exception {
    	return this.getObjectMetadata(context.getSourceClient(), context.getSourceSSEKey(),
                context.getOptions().getSourceBucket(), key);
    }

    protected ObjectMetadata getDestinationObjectMetadata(String key) throws Exception {
    	return this.getObjectMetadata(context.getDestinationClient(), context.getDestinationSSEKey(),
                context.getOptions().getDestinationBucket(), key);
    }     

    private AccessControlList getAccessControlList(AmazonS3 client, SSECustomerKey sseKey, String bucket, String key) throws Exception {
        MirrorOptions options = context.getOptions();
        Exception ex = null;

        for (int tries=0; tries<=options.getMaxRetries(); tries++) {
            try {
                GetObjectAclRequest getObject = new GetObjectAclRequest(bucket, key);

                context.getStats().s3getCount.incrementAndGet();
                return client.getObjectAcl(getObject);

            } catch (Exception e) {
                ex = e;

                if (tries >= options.getMaxRetries()) {
                    // Annoyingly there can be two reasons for this to fail. It will fail if the IAM account
                    // permissions are wrong, but it will also fail if we are copying an item that we don't
                    // own ourselves. This may seem unusual, but it occurs when copying AWS Detailed Billing
                    // objects since although they live in your bucket, the object owner is AWS.
                    getLog().warn("Unable to obtain object ACL, copying item without ACL data.");
                    return new AccessControlList();
                }

                if (options.isVerbose()) {
                   if (tries >= options.getMaxRetries()) {
                        getLog().warn("getObjectAcl(" + key + ") failed (try #" + tries + "), giving up.");
			break;
                    } else {
                        getLog().warn("getObjectAcl("+key+") failed (try #"+tries+"), retrying...");
                    }
                }
            }
        }
        throw ex;
    }
    
    protected AccessControlList getSourceAccessControlList(String key) throws Exception {
    	return this.getAccessControlList(context.getSourceClient(), context.getSourceSSEKey(), context.getOptions().getSourceBucket(), key);
    }

    @SneakyThrows
    protected void logMetadata(String label, ObjectMetadata metadata) {
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

    /*
       This does two things: For CSE it searches for the unencrypted length of the object, so that we can correct the
       Content-Length which includes the encryption overhead. The X-Amz-Unencrypted-Content-Length is created when the
       object is uploaded. Then it removes all user metadata entries which we don't need in the destination object
       anymore and which might even confuse some users and programs. This is relevant at least when CSE is in use at the
       source side as some special headers are used for storing the CEK, IV, etc. Last it enables SSE if requested.

       At the time of writing the Amazon CSE uses the following headers:

       X-Amz-Meta-X-Amz-Key-V2
       X-Amz-Meta-X-Amz-Wrap-Alg
       X-Amz-Meta-X-Amz-Unencrypted-Content-Length
       X-Amz-Meta-X-Amz-Cek-Alg
       X-Amz-Meta-X-Amz-Tag-Len
       X-Amz-Meta-X-Amz-Iv
       X-Amz-Meta-X-Amz-Matdesc

       For a description of the keys see:
       https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/package-summary.html

       This document also list the following additional headers used by CSE v1:

       X-Amz-Key

       X-Amz-Meta-X-Amz-Unencrypted-Content-Length is marked as optional but in https://github.com/aws/aws-sdk-java/issues/1057
       an Amazon developer says that it should always be there when using the AWS Java SDK to create the object. Keeping
       fingers crossed as we really need it.

     */
    protected void adjustDestinationMetadata(ObjectMetadata metadata) {
        Map<String,String> userMetadataMap = metadata.getUserMetadata();
        String length = null;

        for (Iterator<Map.Entry<String,String>> it = userMetadataMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, String> entry = it.next();

            if (length != null && entry.getKey().toLowerCase().equals("x-amz-unencrypted-content-length")) {
                length = entry.getValue();
            }

            if (entry.getKey().matches(USER_METADATA_CLEANUP_REGEXP)) {
                it.remove();
            }
        }

        if (length != null)
            metadata.setContentLength(Long.parseLong(length));

        metadata.setUserMetadata(userMetadataMap);

        if (context.getOptions().getDestinationProfile().getEncryption() == MirrorEncryption.SSE_S3) {
            metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }
    }

    protected void setupSSEEncryption(GetObjectRequest request, SSECustomerKey sseKey) {
        request.setSSECustomerKey(sseKey);
    }

    protected void setupSSEEncryption(GetObjectMetadataRequest request, SSECustomerKey sseKey) {
        request.setSSECustomerKey(sseKey);
    }

    protected void setupSSEEncryption(PutObjectRequest request, SSECustomerKey key) {
        request.setSSECustomerKey(key);
    }

    protected void setupSSEEncryption(CopyObjectRequest request, SSECustomerKey sourceKey, SSECustomerKey destinationKey) {
        request.setSourceSSECustomerKey(sourceKey);
        request.setDestinationSSECustomerKey(destinationKey);
    }

    protected void setupSSEEncryption(CopyPartRequest request, SSECustomerKey sourceKey, SSECustomerKey destinationKey) {
        request.setSourceSSECustomerKey(sourceKey);
        request.setDestinationSSECustomerKey(destinationKey);
    }

    protected void setupSSEEncryption(UploadPartRequest request, SSECustomerKey key) {
        request.setSSECustomerKey(key);
    }
}
