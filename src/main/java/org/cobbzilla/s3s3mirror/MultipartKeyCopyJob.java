package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MultipartKeyCopyJob extends KeyCopyJob {

    public MultipartKeyCopyJob(MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(context, summary, notifyLock);
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
            log.error("error getting metadata for key: " + key + ": " + e);
            return false;
        }
        long objectSize = getRealObjectSize(sourceMetadata);
        if (verbose) logMetadata("source", sourceMetadata);
        final ObjectMetadata destinationMetadata = buildDestinationMetadata(sourceMetadata);
        if (verbose) {
            logMetadata("destination", destinationMetadata);

            log.info("Initiating multipart upload request for " + summary.getKey());
        }

        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(destinationBucket, keydest)
                                                            .withObjectMetadata(destinationMetadata)
                                                            .withStorageClass(StorageClass.valueOf(options.getStorageClass()));

        if (options.isCrossAccountCopy() || !useCopy()) {
            initiateRequest.withCannedACL(CannedAccessControlList.BucketOwnerFullControl);
        } else {
            AccessControlList objectAcl;

            try {
                objectAcl = getSourceAccessControlList(key);
            } catch (Exception e) {
                log.error("error getting ACL for key: " + key + ": " + e);
                return false;
            }

            initiateRequest.setAccessControlList(objectAcl);
        }

        setupSSEEncryption(initiateRequest, context.getDestinationSSEKey());

        InitiateMultipartUploadResult initResult = context.getDestinationClient().initiateMultipartUpload(initiateRequest);

        List<PartETag> partETags = new ArrayList<PartETag>();
        long partSize = options.getUploadPartSize();
        long bytePosition = 0;
        String infoMessage;
      
        if (useCopy()) {
            for (int i = 1; bytePosition < objectSize; i++) {
            	long lastByte = Math.min(objectSize - 1, bytePosition + partSize - 1);
            	long currentPartSize = Math.min(objectSize - bytePosition, partSize);
            
                infoMessage = "copying : " + bytePosition + " to " + lastByte + " (partSize " + currentPartSize + ")";
                if (verbose) log.info(infoMessage);
        		
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

                for (int tries = 1; tries <= maxPartRetries; tries++) {
                    try {
                        if (verbose) log.info("try :" + tries);
                        
                        stats.s3copyCount.incrementAndGet();
                        CopyPartResult copyPartResult = context.getDestinationClient().copyPart(copyRequest);
                        partETags.add(copyPartResult.getPartETag());
                        
                        if (verbose) log.info("completed " + infoMessage);
                        break;
                    } catch (Exception e) {
                        if (tries == maxPartRetries) {
                            context.getDestinationClient().abortMultipartUpload(new AbortMultipartUploadRequest(
                                    destinationBucket, keydest, initResult.getUploadId()));
                            log.error("Exception while doing multipart copy", e);
                            return false;
                        }
                    }
                }  	        	
                
                bytePosition += partSize;
            }
        } else {
            final GetObjectRequest getRequest =  new GetObjectRequest(sourceBucket, key);

            setupSSEEncryption(getRequest, context.getSourceSSEKey());

            stats.s3getCount.incrementAndGet();
            S3Object object = context.getSourceClient().getObject(getRequest);
            InputStream objectStream = object.getObjectContent();

            /*
             * If performance at this point becomes a problem we'll have to look into replacing this with
             * AmazonS3EncryptionClient.uploadObject for CSE. uploadObject also exists for AmazonS3Client but its
             * package private there :(
             */
            for (int i = 1; bytePosition < objectSize; i++) {
            	long lastByte = Math.min(objectSize - 1, bytePosition + partSize - 1);
            	long currentPartSize = Math.min(objectSize - bytePosition, partSize);
            	boolean isLast = (bytePosition + partSize) >= objectSize;
                    		
        		infoMessage = "uploading : " + bytePosition + " to " + lastByte + " (partSize " + currentPartSize
                        + ", isLast " + isLast + ")";
                if (verbose) log.info(infoMessage);

	            UploadPartRequest uploadRequest = new UploadPartRequest()
	            		                             .withBucketName(destinationBucket)
	            		                             .withKey(keydest)
	            		                             .withUploadId(initResult.getUploadId())
	            		                             .withInputStream(objectStream)
	            		                             .withPartSize(currentPartSize)
	            		                             .withPartNumber(i)
	                                                 .withLastPart(isLast);

                setupSSEEncryption(uploadRequest, context.getDestinationSSEKey());
            	
                for (int tries = 1; tries <= maxPartRetries; tries++) {
                    try {
                        if (verbose) log.info("try :" + tries);
                        	
                        stats.s3putCount.incrementAndGet();
                        UploadPartResult uploadPartResult = context.getDestinationClient().uploadPart(uploadRequest);
                        partETags.add(uploadPartResult.getPartETag());
                        
                        if (verbose) log.info("completed " + infoMessage);
                        break;
                    } catch (Exception e) {
                        if (tries == maxPartRetries) {
                            context.getDestinationClient().abortMultipartUpload(new AbortMultipartUploadRequest(
                                    destinationBucket, keydest, initResult.getUploadId()));
                            log.error("Exception while doing multipart copy", e);
                            return false;
                        }
                    }
                }  	        	
                
                bytePosition += partSize;
            }

            try {
                objectStream.close();
            } catch (Exception e) {
                log.error("Exception while trying to close input data stream", e);
                return false;
            }
        }
        
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(destinationBucket, keydest,
                initResult.getUploadId(), partETags);
        context.getDestinationClient().completeMultipartUpload(completeRequest);
        
        stats.bytesCopied.addAndGet(objectSize);
        if(verbose) log.info("completed multipart request for : " + summary.getKey());
        
        return true;
    }
}
