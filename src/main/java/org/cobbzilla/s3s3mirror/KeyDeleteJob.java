package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

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
                log.info("Would have deleted "+key+" from destination because "+keysrc+" does not exist in source");
            } else {
                boolean deletedOK = false;
                for (int tries=0; tries<maxRetries; tries++) {
                    if (verbose) log.info("deleting (try #"+tries+"): "+key);
                    try {
                        stats.s3deleteCount.incrementAndGet();
                        context.getDestinationClient().deleteObject(request);
                        deletedOK = true;
                        if (verbose) log.info("successfully deleted (on try #"+tries+"): "+key);
                        break;

                    } catch (AmazonS3Exception s3e) {
                        // This is really ugly: The AWS Java SDK tries to delete a special key containing optional encryption
                        // materials when deleting the corresponding key and CSE is used. At least with Google's server
                        // implementation this leads to an exception which we try to detect here and then go on to ignore
                        // this error.
                        // 404 is NoSuchKey
                        // "No such object: to-bucket/testDeleteRemoved_Jwp9wSm2zf_1523361645785-dest0.instruction"
                        if (s3e.getStatusCode() == 404 &&
                                s3e.getAdditionalDetails() != null &&
                                s3e.getAdditionalDetails().containsKey("Details") &&
                                s3e.getAdditionalDetails().get("Details").matches("^No such object: .*\\.instruction$")) {
                            deletedOK = true;
                            break;
                        }

                        log.error("s3 exception deleting (try #"+tries+") "+key+": "+s3e);

                    } catch (Exception e) {
                        log.error("unexpected exception deleting (try #"+tries+") "+key+": "+e);
                    }

                    if (Sleep.sleep(10)) break;
                }
                if (deletedOK) {
                    context.getStats().objectsDeleted.incrementAndGet();
                } else {
                    context.getStats().deleteErrors.incrementAndGet();
                }
            }

        } catch (Exception e) {
            log.error("error deleting key: "+key+": "+e);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (verbose) log.info("done with "+key);
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

        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (verbose) log.info("Key not found in source bucket (will delete from destination): "+ keysrc);
                return true;
            } else {
                log.warn("Error getting metadata for " + options.getSourceBucket() + "/" + keysrc + " (not deleting): " + e);
                return false;
            }
        } catch (Exception e) {
            log.warn("Error getting metadata for " + options.getSourceBucket() + "/" + keysrc + " (not deleting): " + e);
            return false;
        }
    }

}
