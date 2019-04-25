package org.cobbzilla.s3s3mirror;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.FileNotFoundException;

@Slf4j
public class KeyDeleteJob extends KeyJob {

    private String keysrc;

    public KeyDeleteJob (MirrorContext context, KeyObjectSummary summary, Object notifyLock) {
        super(context, summary, notifyLock);

        final MirrorOptions options = context.getOptions();
        keysrc = summary.getKey(); // NOTE: summary.getKey is the key in the destination bucket
        if (options.hasSourcePrefix()) {
            keysrc = keysrc.substring(options.getDestinationPrefixLength());
            keysrc = options.getSourcePrefix() + keysrc;
        }
    }

    @Override public Logger getLog() { return log; }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final MirrorStats stats = context.getStats();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();
        final String key = summary.getKey();
        try {
            if (!shouldDelete()) return;

            final DeleteObjectRequest request = new DeleteObjectRequest(options.getDestinationBucket(), key);

            if (options.isDryRun()) {
                log.info("Would have deleted {} from destination because {} does not exist in source bucket.", key, keysrc);
            } else {
                boolean deletedOK = false;
                for (int tries = 1; tries <= maxRetries; tries++) {
                    if (verbose) log.info("Deleting {} (try #{}).", key, tries);
                    try {
                        stats.s3deleteCount.incrementAndGet();
                        context.getDestinationClient().deleteObject(request);
                        deletedOK = true;
                        if (verbose) log.info("Successfully deleted {} (try #{}).", key, tries);
                        break;

                    } catch (AmazonS3Exception e) {
                        // This is really ugly: The AWS Java SDK tries to delete a special key containing optional encryption
                        // materials when deleting the corresponding key and CSE is used. At least with Google's server
                        // implementation this leads to an exception which we try to detect here and then go on to ignore
                        // this error.
                        // 404 is NoSuchKey
                        // "No such object: to-bucket/testDeleteRemoved_Jwp9wSm2zf_1523361645785-dest0.instruction"
                        if (e.getStatusCode() == 404 &&
                                e.getAdditionalDetails() != null &&
                                e.getAdditionalDetails().containsKey("Details") &&
                                e.getAdditionalDetails().get("Details").matches("^No such object: .*\\.instruction$")) {
                            deletedOK = true;
                            break;
                        }

                        log.error("S3 exception deleting {} (try #{}).", key, tries, e);
                    } catch (SdkClientException e) {
                        log.error("Client exception deleting {} (try #{}).", key, tries, e);
                    }

                    if (tries < maxRetries && Sleep.sleep(10)) break;
                }
                if (deletedOK) {
                    context.getStats().objectsDeleted.incrementAndGet();
                } else {
                    context.getStats().deleteErrors.incrementAndGet();
                }
            }

        } catch (Exception e) {
            log.error("Error deleting key {}.", key, e);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (verbose) log.info("Done with {}.", key);
        }
    }

    private boolean shouldDelete() {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();

        // Does it exist in the source bucket
        try {
            @SuppressWarnings("unused")
			ObjectMetadata metadata = getSourceObjectMetadata(keysrc);
            return false; // object exists in source bucket, don't delete it from destination bucket

        } catch (FileNotFoundException e) {
            if (verbose) log.info("Key {} not found in source bucket (will delete from destination).", keysrc);
            return true;
        } catch (SdkClientException e) {
            log.warn("Error getting metadata for {}/{} (not deleting).", options.getSourceBucket(), keysrc, e);
            return false;
        }
    }

}
