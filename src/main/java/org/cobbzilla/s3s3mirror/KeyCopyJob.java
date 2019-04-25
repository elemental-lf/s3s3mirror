package org.cobbzilla.s3s3mirror;

import com.amazonaws.ResetException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.FileNotFoundException;
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
                log.info("Would have copied {} to destination {}.", key, keydest);
            } else {
                if (copyKey()) {
                    context.getStats().objectsCopied.incrementAndGet();
                } else {
                    context.getStats().copyErrors.incrementAndGet();
                }
            }
        } catch (Exception e) {
            log.error("Error copying key {}.", key, e);
        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (options.isVerbose()) log.info("Done with {}.", key);
        }
    }

    boolean copyKey() {
        String key = summary.getKey();
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();
        int maxRetries= options.getMaxRetries();
        MirrorStats stats = context.getStats();

        final ObjectMetadata sourceMetadata;
        try {
            sourceMetadata = getSourceObjectMetadata(key);
        } catch (FileNotFoundException e) {
            log.error("Key {} not found anymore.", key, e);
            return false;
        }
        if (verbose) logMetadata("source", sourceMetadata);
		final ObjectMetadata destinationMetadata = buildDestinationMetadata(sourceMetadata);
        if (verbose) logMetadata("destination ", destinationMetadata);

        boolean copyOkay = false;
        for (int tries = 1; tries <= maxRetries; tries++) {
            S3ObjectInputStream objectStream = null;
            try {
            	if (useCopy()) {
                    if (verbose) log.info("Copying to {} (try #{}).", keydest, tries);

            		final CopyObjectRequest copyRequest = new CopyObjectRequest(options.getSourceBucket(), key, options.getDestinationBucket(), keydest)
            											  .withStorageClass(StorageClass.valueOf(options.getStorageClass()))
            											  .withNewObjectMetadata(destinationMetadata);

                    setupSSEEncryption(copyRequest, context.getSourceSSEKey(), context.getDestinationSSEKey());

                    if (options.isCrossAccountCopy()) {
                        copyRequest.setCannedAccessControlList(CannedAccessControlList.BucketOwnerFullControl);
                    } else {
                        final AccessControlList objectAcl = getSourceAccessControlList(key);
                        copyRequest.setAccessControlList(objectAcl);
                    }

                    stats.s3copyCount.incrementAndGet();
                    context.getSourceClient().copyObject(copyRequest);

                    if (verbose) log.info("Completed copying to {}.", keydest);
            	} else {
                    if (verbose) log.info("Uploading to {} (try #{}).", keydest, tries);

            		final GetObjectRequest getRequest =  new GetObjectRequest(options.getSourceBucket(), key);

                    setupSSEEncryption(getRequest, context.getSourceSSEKey());

                    stats.s3getCount.incrementAndGet();
            		S3Object object = context.getSourceClient().getObject(getRequest);
                    objectStream = object.getObjectContent();

            		final PutObjectRequest putRequest = new PutObjectRequest(options.getDestinationBucket(), keydest, objectStream, destinationMetadata)
            												.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl)
            												.withStorageClass(StorageClass.valueOf(options.getStorageClass()));

                    setupSSEEncryption(putRequest, context.getDestinationSSEKey());

            		stats.s3putCount.incrementAndGet();
                    context.getDestinationClient().putObject(putRequest);
                    // Stream is closed when we reached EOF
                    objectStream = null;

                    if (verbose) log.info("Completed uploading to {}.", keydest);
            	}

            	stats.bytesCopied.addAndGet(getRealObjectSize(sourceMetadata));


                copyOkay = true;
                break;
            } catch (ResetException e) {
                // ResetException can occur when there is a transient, retryable failure.
                if (verbose) log.info("Reset exception copying to {} (try#{}).", keydest, tries, e);
            } catch (SdkClientException e) {
                log.error("Client exception copying to {} (try#{}).", keydest, tries, e);
            } finally {
                if (objectStream != null) {
                    this.closeS3ObjectInputStream(objectStream);
                }
            }

            if (tries < maxRetries && Sleep.sleep(50)) break;
        }

        if (!copyOkay) {
            log.error("Giving up on copying to {}.", keydest);
        }

        return copyOkay;
    }

    private boolean shouldTransfer() {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        final boolean verbose = options.isVerbose();
        final boolean compareSize = options.isCompareSize();

        if (options.hasCtime()) {
            final Date lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) log.info("No Last-Modified header for key {}/{}.", options.getSourceBucket(), key);

            } else {
                if (lastModified.getTime() < options.getMaxAge()) {
                    if (verbose) log.info("Key {} (last modified {}) is older than {} (cutoff {}), not copying.", key,
                            lastModified, options.getCtime(), options.getMaxAgeDate());
                    return false;
                }
            }
        }

        final ObjectMetadata destinationMetadata;
        try {
            destinationMetadata = getDestinationObjectMetadata(keydest);
        } catch (FileNotFoundException e) {
            if (verbose) log.info("Key {} not found in destination bucket (will copy).", keydest);
            return true;
        } catch (SdkClientException e) {
            log.warn("Error getting metadata for {}/{} (not copying).", options.getDestinationBucket(), keydest, e);
            return false;
        }

        if (compareSize) {
            final ObjectMetadata sourceMetadata;
            try {
                sourceMetadata = getSourceObjectMetadata(key);
            } catch (FileNotFoundException e) {
                if (verbose) log.info("Key {}/{} not found in source bucket anymore (not copying).",
                        options.getSourceBucket(), key);
                return false;
            } catch (SdkClientException e) {
                log.warn("Error getting metadata for {}/{} (not copying).", options.getSourceBucket(), key, e);
                return false;
            }

            final boolean sizeChanged = getRealObjectSize(sourceMetadata) != getRealObjectSize(destinationMetadata);

            if (sizeChanged) log.info("Object size changed for {}/{} (copying).", options.getSourceBucket(), key);

            return sizeChanged;
        } else {
            if (verbose) log.info("Destination object {}/{} already exists, not copying.",
                    options.getDestinationBucket(), keydest);
            return false;
        }
    }

    boolean useCopy() {
        return context.getSourceClient() == context.getDestinationClient()
                && !MirrorEncryption.isCSE(context.getOptions().getSourceProfile().getEncryption());
    }
}
