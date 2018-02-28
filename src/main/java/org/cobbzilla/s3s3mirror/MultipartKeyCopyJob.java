package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MultipartKeyCopyJob extends KeyCopyJob {

    public MultipartKeyCopyJob(MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(context, summary, notifyLock);
    }

    @Override
    boolean keyCopied(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
    	String key = summary.getKey();
        long objectSize = summary.getSize();
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();
        String sourceBucket = options.getSourceBucket();
        int maxPartRetries = options.getMaxRetries();
        MirrorStats stats = context.getStats();
        String destinationBucket = options.getDestinationBucket();
        
		final ObjectMetadata destinationMetadata = sourceMetadata.clone();

        if (options.getSourceProfile().getEncryption() == MirrorEncryption.SSE_AES_256) {
            destinationMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }
    
        if (verbose) log.info("Initiating multipart upload request for " + summary.getKey());
        
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(destinationBucket, keydest)
                .withObjectMetadata(destinationMetadata);

        if (options.isCrossAccountCopy() || (context.getSourceClient() != context.getDestinationClient())) {
            initiateRequest.withCannedACL(CannedAccessControlList.BucketOwnerFullControl);
        } else {
            initiateRequest.withAccessControlList(objectAcl);
        }

        InitiateMultipartUploadResult initResult = context.getDestinationClient().initiateMultipartUpload(initiateRequest);

        List<PartETag> partETags = new ArrayList<PartETag>();
        long partSize = Math.max(options.getUploadPartSize(), (long)Math.pow(2.0, 20.0));
        long bytePosition = 0;
        String infoMessage;
      
        // Source and destination are using the same client connection -> use copy
        if (context.getSourceClient() == context.getDestinationClient()) {
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

                if (context.getSourceSSEKey() != null) copyRequest.setSourceSSECustomerKey(context.getSourceSSEKey());
                if (context.getDestinationSSEKey() != null) copyRequest.setDestinationSSECustomerKey(context.getDestinationSSEKey());

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
            final GetObjectRequest getRequest =  new GetObjectRequest(options.getSourceBucket(), key);

            if (context.getSourceSSEKey() != null) getRequest.setSSECustomerKey(context.getSourceSSEKey());

            stats.s3getCount.incrementAndGet();
            S3Object object = context.getSourceClient().getObject(getRequest); 
    		
            for (int i = 1; bytePosition < objectSize; i++) {
            	long lastByte = Math.min(objectSize - 1, bytePosition + partSize - 1);
            	long currentPartSize = Math.min(objectSize - bytePosition, partSize);
                    		
        		infoMessage = "uploading : " + bytePosition + " to " + lastByte + " (partSize " + currentPartSize + ")";
                if (verbose) log.info(infoMessage);
        		
	            UploadPartRequest uploadRequest = new UploadPartRequest()
	            		                             .withBucketName(destinationBucket)
	            		                             .withKey(keydest)
	            		                             .withUploadId(initResult.getUploadId())
	            		                             .withInputStream(object.getObjectContent())
	            		                             .withPartSize(currentPartSize)
	            		                             .withPartNumber(i);

                if (context.getDestinationSSEKey() != null) uploadRequest.setSSECustomerKey(context.getDestinationSSEKey());
            	
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
                            try {
                            	object.getObjectContent().close();
                            } catch (Exception e2) {
                            	log.error("Exception while trying to close input data stream", e2);
                            }
                            return false;
                        }
                    }
                }  	        	
                
                bytePosition += partSize;
            }
            
            try {
            	object.getObjectContent().close();
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

    @Override
    boolean objectChanged(ObjectMetadata metadata) {
        return summary.getSize() != metadata.getContentLength();
    }
}
