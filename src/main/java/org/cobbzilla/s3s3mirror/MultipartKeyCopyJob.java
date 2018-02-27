package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
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
        long objectSize = summary.getSize();
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();
        String sourceBucketName = options.getSourceBucket();
        int maxPartRetries = options.getMaxRetries();
        String targetBucketName = options.getDestinationBucket();
        
		final ObjectMetadata destinationMetadata = sourceMetadata.clone();
		
        if (options.isEncrypt()) {
        	destinationMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);   
		}	
    
        List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
        
        if (verbose) log.info("Initiating multipart upload request for " + summary.getKey());
        
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(targetBucketName, keydest)
                .withObjectMetadata(sourceMetadata);

        if (options.isCrossAccountCopy()) {
            initiateRequest.withCannedACL(CannedAccessControlList.BucketOwnerFullControl);
        } else {
            initiateRequest.withAccessControlList(objectAcl);
        }

        InitiateMultipartUploadResult initResult = context.getDestinationClient().initiateMultipartUpload(initiateRequest);

        long partSize = options.getUploadPartSize();
        long bytePosition = 0;

        for (int i = 1; bytePosition < objectSize; i++) {
            long lastByte = bytePosition + partSize - 1 >= objectSize ? objectSize - 1 : bytePosition + partSize - 1;
            
            String infoMessage = "copying : " + bytePosition + " to " + lastByte;
            if (verbose) log.info(infoMessage);
            
            CopyPartRequest copyRequest = new CopyPartRequest()
                    .withDestinationBucketName(targetBucketName)
                    .withDestinationKey(keydest)
                    .withSourceBucketName(sourceBucketName)
                    .withSourceKey(summary.getKey())
                    .withUploadId(initResult.getUploadId())
                    .withFirstByte(bytePosition)
                    .withLastByte(lastByte)
                    .withPartNumber(i);

            for (int tries = 1; tries <= maxPartRetries; tries++) {
                try {
                    if (options.isVerbose()) log.info("try :" + tries);
                    context.getStats().s3copyCount.incrementAndGet();
                    CopyPartResult copyPartResult = context.getDestinationClient().copyPart(copyRequest);
                    copyResponses.add(copyPartResult);
                    if (verbose) log.info("completed " + infoMessage);
                    break;
                } catch (Exception e) {
                    if (tries == maxPartRetries) {
                        context.getDestinationClient().abortMultipartUpload(new AbortMultipartUploadRequest(
                                targetBucketName, keydest, initResult.getUploadId()));
                        log.error("Exception while doing multipart copy", e);
                        return false;
                    }
                }
            }
            
            bytePosition += partSize;
        }
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(targetBucketName, keydest,
                initResult.getUploadId(), getETags(copyResponses));
        context.getDestinationClient().completeMultipartUpload(completeRequest);
        
        context.getStats().bytesCopied.addAndGet(objectSize);
        if(verbose) log.info("completed multipart request for : " + summary.getKey());
        
        return true;
    }

    private List<PartETag> getETags(List<CopyPartResult> copyResponses) {
        List<PartETag> eTags = new ArrayList<PartETag>();
        for (CopyPartResult response : copyResponses) {
            eTags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return eTags;
    }

    @Override
    boolean objectChanged(ObjectMetadata metadata) {
        return summary.getSize() != metadata.getContentLength();
    }
}
