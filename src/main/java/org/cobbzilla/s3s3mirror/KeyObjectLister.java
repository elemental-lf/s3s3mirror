package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KeyObjectLister extends KeyLister {

    private MirrorContext context;
    private AmazonS3 client;
    private int maxQueueCapacity;

    private final List<S3ObjectSummary> summaries;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private ListObjectsRequest request;
    private ObjectListing listing;

    @Override
    public boolean isDone () { return done.get(); }

    public KeyObjectLister(MirrorContext context, int maxQueueCapacity, MirrorProfile profile, AmazonS3 client, String bucket, String prefix) {
        this.context = context;
        this.client = client;
        this.maxQueueCapacity = maxQueueCapacity;

        final MirrorOptions options = context.getOptions();
        int fetchSize = options.getMaxThreads();
        this.summaries = new ArrayList<S3ObjectSummary>(10*fetchSize);

        this.request = new ListObjectsRequest(bucket, prefix, null, null, fetchSize);
        if (profile.hasOption(MirrorProfileOptions.NO_ENCODING_TYPE))
            this.request.setEncodingType(Constants.NO_ENCODING_TYPE);
        listing = s3getFirstBatch();
        synchronized (summaries) {
            final List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
            summaries.addAll(objectSummaries);
            context.getStats().objectsRead.addAndGet(objectSummaries.size());
            if (options.isVerbose()) log.info("Added initial set of {} keys.", objectSummaries.size());
        }
    }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        int counter = 0;
        log.info("starting...");
        try {
            while (true) {
                while (getSize() < maxQueueCapacity) {
                    if (listing.isTruncated()) {
                        listing = s3getNextBatch();
                        if (++counter % 100 == 0) context.getStats().logStats();
                        synchronized (summaries) {
                            final List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
                            summaries.addAll(objectSummaries);
                            context.getStats().objectsRead.addAndGet(objectSummaries.size());
                            if (verbose) log.info("queued next set of "+objectSummaries.size()+" keys (total now="+getSize()+")");
                        }

                    } else {
                        log.info("No more keys found in source bucket, exiting.");
                        return;
                    }
                }
                if (Sleep.sleep(50)) return;
            }
        } catch (Exception e) {
            log.error("Error in run loop, KeyLister thread now exiting.", e);

        } finally {
            if (verbose) log.info("KeyLister run loop finished.");
            done.set(true);
        }
    }

    private ObjectListing s3getFirstBatch() {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();

        Exception lastException = null;
        for (int tries = 1; tries <= maxRetries; tries++) {
            try {
                context.getStats().s3getCount.incrementAndGet();
                ObjectListing listing = client.listObjects(request);
                if (verbose) log.info("Successfully got first batch of objects (on try #{}).", tries);
                return listing;

            } catch (Exception e) {
                lastException = e;
                log.warn("s3getFirstBatch: Error listing (try #{}).", tries, e);
                if (Sleep.sleep(50)) break;
            }
        }
        throw new IllegalStateException("s3getFirstBatch failed even after " + maxRetries + ": " + lastException + ".");
    }

    private ObjectListing s3getNextBatch() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();

        for (int tries = 1; tries <= maxRetries; tries++) {
            try {
                context.getStats().s3getCount.incrementAndGet();
                ObjectListing next = client.listNextBatchOfObjects(listing);
                if (verbose) log.info("Successfully got next batch of objects (on try #{}).", tries);
                return next;

            } catch (AmazonS3Exception s3e) {
                log.error("S3 exception listing objects (try #{}).", tries, s3e);

            } catch (Exception e) {
                log.error("Unexpected exception listing objects (try #{}).", tries, e);
            }
            if (Sleep.sleep(50)) break;
        }
        throw new IllegalStateException("Too many errors trying to list objects (maxRetries="+maxRetries+").");
    }

    @Override
    protected int getSize() {
        synchronized (summaries) {
            return summaries.size();
        }
    }

    @Override
    public List<KeyObjectSummary> getNextBatch() {
        List<KeyObjectSummary> copy;
        synchronized (summaries) {
            copy = KeyObjectSummary.S3ObjectSummaryToKeyObject(summaries);
            summaries.clear();
        }
        return copy;
    }
}
