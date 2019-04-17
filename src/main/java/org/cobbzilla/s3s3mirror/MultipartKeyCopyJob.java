package org.cobbzilla.s3s3mirror;

import com.amazonaws.ResetException;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.toIntExact;

@Slf4j
public class MultipartKeyCopyJob extends KeyCopyJob {

    public MultipartKeyCopyJob(MirrorContext context, KeyObjectSummary summary, Object notifyLock) {
        super(context, summary, notifyLock);
    }

    private InitiateMultipartUploadResult setupMultipartUpload(ObjectMetadata destinationMetadata, AccessControlList destinationAcl) {
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();
        String destinationBucket = options.getDestinationBucket();

        if (verbose) log.info("Initiating multipart upload request for {} with size {}.", keydest, getRealObjectSize(destinationMetadata));

        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(destinationBucket, keydest)
                .withObjectMetadata(destinationMetadata)
                .withStorageClass(StorageClass.valueOf(options.getStorageClass()));

        if (destinationAcl == null) {
            initiateRequest.withCannedACL(CannedAccessControlList.BucketOwnerFullControl);
        } else {
            initiateRequest.setAccessControlList(destinationAcl);
        }

        setupSSEEncryption(initiateRequest, context.getDestinationSSEKey());

        return context.getDestinationClient().initiateMultipartUpload(initiateRequest);
    }

    @Override
    boolean keyCopied() {
    	String key = summary.getKey();
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();
        String sourceBucket = options.getSourceBucket();
        int maxPartRetries = options.getMaxRetries();
        MirrorStats stats = context.getStats();
        String destinationBucket = options.getDestinationBucket();


        final ObjectMetadata sourceMetadata;
        try {
            sourceMetadata = getSourceObjectMetadata(key);
        } catch (Exception e) {
            log.error("Error getting metadata for key {}.", key, e);
            return false;
        }
        long objectSize = getRealObjectSize(sourceMetadata);
        final ObjectMetadata destinationMetadata = buildDestinationMetadata(sourceMetadata);

        if (verbose) {
            logMetadata("source", sourceMetadata);
            logMetadata("destination", destinationMetadata);
        }

        AccessControlList destinationAcl = null;
        if (!options.isCrossAccountCopy() && useCopy()) {
            try {
                destinationAcl = getSourceAccessControlList(key);
            } catch (Exception e) {
                log.error("Error getting ACL for key {}.", key, e);
                return false;
            }
        }

        List<PartETag> partETags = new ArrayList<PartETag>();
        long partSize = options.getUploadPartSize();
        InitiateMultipartUploadResult initResult = null;

        if (useCopy()) {
            initResult = setupMultipartUpload(destinationMetadata, destinationAcl);
            long bytePosition = 0;
            for (int i = 1; bytePosition < objectSize; i++) {
            	long lastByte = Math.min(objectSize - 1, bytePosition + partSize - 1);
            	long currentPartSize = Math.min(objectSize - bytePosition, partSize);
            


                CopyPartRequest copyRequest = new CopyPartRequest()
                							  .withDestinationBucketName(destinationBucket)
                							  .withDestinationKey(keydest)
                							  .withSourceBucketName(sourceBucket)
                                              .withSourceKey(key)
                							  .withUploadId(initResult.getUploadId())
                							  .withFirstByte(bytePosition)
                							  .withLastByte(lastByte)
                							  .withPartNumber(i);

                setupSSEEncryption(copyRequest, context.getSourceSSEKey(), context.getDestinationSSEKey());

                boolean copyPartOkay = false;
                for (int tries = 1; tries <= maxPartRetries; tries++) {
                    try {
                        if (verbose) log.info("Copying to {}: {} to {} (currentPartSize {}, try#{})", keydest, bytePosition, lastByte,
                                currentPartSize, tries);

                        stats.s3copyCount.incrementAndGet();
                        CopyPartResult copyPartResult = context.getDestinationClient().copyPart(copyRequest);
                        partETags.add(copyPartResult.getPartETag());
                        
                        if (verbose) log.info("Completed copying to {}: {} to {} (currentPartSize {})", keydest,
                                bytePosition, lastByte, currentPartSize);
                        copyPartOkay = true;
                        break;
                    } catch (ResetException e) {
                        // ResetException can occur when there is a transient, retryable failure.
                        if (verbose) log.info("Reset exception copying to {} (try#{}).", keydest, tries, e);
                    } catch (AmazonS3Exception e) {
                        log.error("S3 exception copying from to {} (try#{}).", keydest, tries, e);
                    } catch (Exception e) {
                        log.error("Unexpected exception copying to {} (try#{}).", keydest, tries, e);
                    }

                    if (Sleep.sleep(50)) break;
                }

                if (!copyPartOkay) {
                    log.error("Giving up on copying part at offset {} to {}.", bytePosition, keydest);
                    context.getDestinationClient().abortMultipartUpload(new AbortMultipartUploadRequest(
                            destinationBucket, keydest, initResult.getUploadId()));
                    return false;
                }

                bytePosition += partSize;
            }
        } else {
            final GetObjectRequest getRequest =  new GetObjectRequest(sourceBucket, key);

            setupSSEEncryption(getRequest, context.getSourceSSEKey());

            stats.s3getCount.incrementAndGet();
            S3Object object = context.getSourceClient().getObject(getRequest);

            boolean uploadOkay = false;
            for (int tries = 1; tries <= maxPartRetries; tries++) {
                S3ObjectInputStream objectStream = null;
                try {
                    if (verbose) log.info("try :" + tries);

                    /*
                     * If performance at this point becomes a problem we'll have to look into replacing this with
                     * AmazonS3EncryptionClient.uploadObject for CSE. uploadObject also exists for AmazonS3Client but its
                     * package private there :(
                     */
                    initResult = setupMultipartUpload(destinationMetadata, destinationAcl);
                    objectStream = object.getObjectContent();
                    long bytePosition = 0;
                    for (int i = 1; bytePosition < objectSize; i++) {
                        long lastByte = Math.min(objectSize - 1, bytePosition + partSize - 1);
                        long currentPartSize = Math.min(objectSize - bytePosition, partSize);
                        boolean isLast = (bytePosition + partSize) >= objectSize;

                        if (verbose) log.info("Uploading {}: {} to {} (currentPartSize {}, isLast {})", keydest,
                                bytePosition, lastByte, currentPartSize, isLast);

                        UploadPartRequest uploadRequest = new UploadPartRequest()
                                .withBucketName(destinationBucket)
                                .withKey(keydest)
                                .withUploadId(initResult.getUploadId())
                                .withInputStream(objectStream)
                                .withPartSize(currentPartSize)
                                .withPartNumber(i)
                                .withLastPart(isLast);

                        uploadRequest.getRequestClientOptions().setReadLimit(toIntExact(currentPartSize) + 1);

                        setupSSEEncryption(uploadRequest, context.getDestinationSSEKey());

                        stats.s3putCount.incrementAndGet();
                        UploadPartResult uploadPartResult = context.getDestinationClient().uploadPart(uploadRequest);
                        partETags.add(uploadPartResult.getPartETag());

                        bytePosition += partSize;
                        if (verbose) log.info("Completed uploading {}: {} to {} (currentPartSize {}, isLast {})", keydest,
                                bytePosition, lastByte, currentPartSize, isLast);
                    }

                    // Stream is closed when we reached EOF
                    objectStream = null;
                    uploadOkay = true;
                    break;
                } catch (ResetException e) {
                    if (initResult != null) {
                        context.getDestinationClient().abortMultipartUpload(new AbortMultipartUploadRequest(
                                destinationBucket, keydest, initResult.getUploadId()));
                    }
                    partETags.clear();
                    // ResetException can occur when there is a transient, retryable failure.
                    if (verbose) log.info("Reset exception uploading to {} (try#{}).", keydest, tries, e);
                } catch (AmazonS3Exception e) {
                    if (initResult != null) {
                        context.getDestinationClient().abortMultipartUpload(new AbortMultipartUploadRequest(
                                destinationBucket, keydest, initResult.getUploadId()));
                    }
                    partETags.clear();
                    log.error("S3 exception uploading to {} (try#{}).", keydest, tries, e);
                } catch (Exception e) {
                    if (initResult != null) {
                        context.getDestinationClient().abortMultipartUpload(new AbortMultipartUploadRequest(
                                destinationBucket, keydest, initResult.getUploadId()));
                    }
                    partETags.clear();
                    log.error("Unexpected exception uploading to {} (try#{}).", keydest, tries, e);
                } finally {
                    if (objectStream != null) {
                        this.closeS3ObjectInputStream(objectStream);
                    }
                }

                if (Sleep.sleep(50)) break;
            }

            if (!uploadOkay) {
                log.error("Giving up on multi-part upload to {}.", keydest);
                return false;
            }
        }

        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(destinationBucket, keydest,
                initResult.getUploadId(), partETags);
        context.getDestinationClient().completeMultipartUpload(completeRequest);
        
        stats.bytesCopied.addAndGet(objectSize);
        if(verbose) log.info("Completed multipart request for {}.", keydest);
        
        return true;
    }
}
