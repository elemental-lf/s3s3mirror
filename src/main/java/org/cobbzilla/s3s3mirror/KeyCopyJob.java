package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.model.*;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.InputStream;
import java.util.Date;

/**
 * Handles a single key. Determines if it should be copied, and if so, performs the copy operation.
 */
@Slf4j
public class KeyCopyJob extends KeyJob {
    protected String keydest;

    public KeyCopyJob(MirrorContext context, KeyObjectSummary summary, Object notifyLock) {
        super(context, summary, notifyLock);

        keydest = summary.getKey();
        final MirrorOptions options = context.getOptions();
        if (options.hasDestinationPrefix()) {
            keydest = keydest.substring(options.getSourcePrefixLength());
            keydest = options.getDestinationPrefix() + keydest;
        }
    }

    @Override public Logger getLog() { return log; }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        try {
            if (!shouldTransfer()) return;

            if (options.isDryRun()) {
                log.info("Would have copied " + key + " to destination: " + keydest);
            } else {
                if (keyCopied()) {
                    context.getStats().objectsCopied.incrementAndGet();
                } else {
                    context.getStats().copyErrors.incrementAndGet();
                }
            }
        } catch (Exception e) {
            log.error("error copying key: " + key + ": " + e);
        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (options.isVerbose()) log.info("done with " + key);
        }
    }

    boolean keyCopied() {
        String key = summary.getKey();
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();
        int maxRetries= options.getMaxRetries();
        MirrorStats stats = context.getStats();

        final ObjectMetadata sourceMetadata;
        try {
            sourceMetadata = getSourceObjectMetadata(key);
        } catch (Exception e) {
            log.error("error getting metadata for key: " + key + ": " + e);
            return false;
        }
        if (verbose) logMetadata("source", sourceMetadata);
		final ObjectMetadata destinationMetadata = buildDestinationMetadata(sourceMetadata);
        if (verbose) logMetadata("destination ", destinationMetadata);

        for (int tries = 0; tries < maxRetries; tries++) {
            if (verbose) log.info("copying (try #" + tries + "): " + key + " to: " + keydest);
            
            try {
            	if (useCopy()) {
            		final CopyObjectRequest copyRequest = new CopyObjectRequest(options.getSourceBucket(), key, options.getDestinationBucket(), keydest)
            											  .withStorageClass(StorageClass.valueOf(options.getStorageClass()))
            											  .withNewObjectMetadata(destinationMetadata);

                    setupSSEEncryption(copyRequest, context.getSourceSSEKey(), context.getDestinationSSEKey());

                    if (options.isCrossAccountCopy()) {
                        copyRequest.setCannedAccessControlList(CannedAccessControlList.BucketOwnerFullControl);
                    } else {
                        AccessControlList objectAcl;

                        try {
                            objectAcl = getSourceAccessControlList(key);
                        } catch (Exception e) {
                            log.error("error getting ACL for key: " + key + ": " + e);
                            return false;
                        }

                        copyRequest.setAccessControlList(objectAcl);
                    }
                    
                    stats.s3copyCount.incrementAndGet();
                    context.getSourceClient().copyObject(copyRequest);
            	} else {        		

            		final GetObjectRequest getRequest =  new GetObjectRequest(options.getSourceBucket(), key);

                    setupSSEEncryption(getRequest, context.getSourceSSEKey());

                    stats.s3getCount.incrementAndGet();
            		S3Object object = context.getSourceClient().getObject(getRequest);
                    @Cleanup InputStream objectStream = object.getObjectContent();

            		final PutObjectRequest putRequest = new PutObjectRequest(options.getDestinationBucket(), keydest, objectStream, destinationMetadata)
            												.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl)
            												.withStorageClass(StorageClass.valueOf(options.getStorageClass()));

                    setupSSEEncryption(putRequest, context.getDestinationSSEKey());

            		stats.s3putCount.incrementAndGet();
                    context.getDestinationClient().putObject(putRequest);
            	}

            	stats.bytesCopied.addAndGet(getRealObjectSize(sourceMetadata));
                if (verbose) log.info("successfully copied (on try #" + tries + "): " + key + " to " + keydest);
                
                return true;               	
            } catch (AmazonS3Exception s3e) {
                log.error("s3 exception copying (try #" + tries + ") " + key + " to " + keydest + ": " + s3e);
            } catch (Exception e) {
                log.error("unexpected exception copying (try #" + tries + ") " + key + " to " + keydest + ": " + e);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error("interrupted while waiting to retry key: " + key);
                return false;
            }
        }
        return false;
    }

    private boolean shouldTransfer() {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        final boolean verbose = options.isVerbose();
        final boolean compareSize = options.isCompareSize();

        if (options.hasCtime()) {
            final Date lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) log.info("No Last-Modified header for key: " + key);

            } else {
                if (lastModified.getTime() < options.getMaxAge()) {
                    if (verbose) log.info("key "+key+" (lastmod="+lastModified+") is older than "+options.getCtime()+" (cutoff="+options.getMaxAgeDate()+"), not copying");
                    return false;
                }
            }
        }

        final ObjectMetadata destinationMetadata;
        try {
            destinationMetadata = getDestinationObjectMetadata(keydest);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (verbose) log.info("Key not found in destination bucket (will copy): "+ keydest);
                return true;
            } else {
                log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
                return false;
            }
        } catch (Exception e) {
            log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
            return false;
        }

        if (compareSize) {
            final ObjectMetadata sourceMetadata;
            try {
                sourceMetadata = getSourceObjectMetadata(key);
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404) {
                    if (verbose) log.info("Key not found in source bucket anymore (not copying): " + key);
                    return false;
                } else {
                    log.warn("Error getting metadata for " + options.getSourceBucket() + "/" + key + " (not copying): " + e);
                    return false;
                }
            } catch (Exception e) {
                log.warn("Error getting metadata for " + options.getSourceBucket() + "/" + key + " (not copying): " + e);
                return false;
            }

            final boolean sizeChanged = getRealObjectSize(sourceMetadata) != getRealObjectSize(destinationMetadata);

            if (sizeChanged) log.info("Object size changed (copying): " + key);

            return sizeChanged;
        } else {
            if (verbose) log.info("Destination object already exists, not copying: "+ keydest);
            return false;
        }
    }

    boolean useCopy() {
        return context.getSourceClient() == context.getDestinationClient()
                && !MirrorEncryption.isCSE(context.getOptions().getSourceProfile().getEncryption());
    }
}
