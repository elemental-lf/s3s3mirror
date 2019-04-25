package org.cobbzilla.s3s3mirror;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KeyVersionLister extends KeyLister {

    private MirrorContext context;
    private AmazonS3 client;
    private int maxQueueCapacity;

    private final List<S3VersionSummary> summaries;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private ListVersionsRequest request;
    private VersionListing listing;

    public boolean isDone () { return done.get(); }

    public KeyVersionLister(MirrorContext context, int maxQueueCapacity, MirrorProfile profile, AmazonS3 client, String bucket, String prefix) {
        this.context = context;
        this.client = client;
        this.maxQueueCapacity = maxQueueCapacity;

        final MirrorOptions options = context.getOptions();
        int fetchSize = options.getMaxThreads();
        this.summaries = new ArrayList<S3VersionSummary>(10*fetchSize);

        this.request = new ListVersionsRequest()
                       .withBucketName(bucket)
                       .withPrefix(prefix)
                       .withMaxResults(fetchSize);
        if (profile.hasOption(MirrorProfileOptions.NO_ENCODING_TYPE))
            this.request.setEncodingType("none");
        listing = s3getFirstBatch();
        synchronized (summaries) {
            final List<S3VersionSummary> objectSummaries = listing.getVersionSummaries();
            summaries.addAll(objectSummaries);
            context.getStats().objectsRead.addAndGet(objectSummaries.size());
            if (options.isVerbose()) log.info("added initial set of "+objectSummaries.size()+" keys");
        }
    }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        int counter = 0;
        log.info("Starting...");
        try {
            while (true) {
                while (getSize() < maxQueueCapacity) {
                    if (listing.isTruncated()) {
                        listing = s3getNextBatch();
                        if (++counter % 100 == 0) context.getStats().logStats();
                        synchronized (summaries) {
                            final List<S3VersionSummary> objectSummaries = listing.getVersionSummaries();
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
            log.error("Error in run loop, KeyLister thread exiting now.", e);
        } finally {
            if (verbose) log.info("KeyLister run loop finished.");
            done.set(true);
        }
    }

    private VersionListing s3getFirstBatch() {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();

        SdkClientException lastException = null;
        for (int tries = 1; tries <= maxRetries; tries++) {
            try {
                context.getStats().s3getCount.incrementAndGet();
                VersionListing listing = client.listVersions(this.request);
                this.request.setKeyMarker(listing.getNextKeyMarker());
                this.request.setVersionIdMarker(listing.getNextVersionIdMarker());
                if (verbose) log.info("Successfully got first batch of objects (try #{}).", tries);
                return listing;
            } catch (SdkClientException e) {
                lastException = e;
                log.error("Client exception while listing objects (try #{}).", tries, e);
            }

            if (tries < maxRetries && Sleep.sleep(50)) break;
        }

        throw new IllegalStateException("s3getFirstBatch failed even after " + maxRetries + ".", lastException);
    }

    private VersionListing s3getNextBatch() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();

        SdkClientException lastException = null;
        for (int tries = 1; tries <= maxRetries; tries++) {
            try {
                context.getStats().s3getCount.incrementAndGet();
                VersionListing next = client.listVersions(this.request);
                this.request.setKeyMarker(listing.getNextKeyMarker());
                this.request.setVersionIdMarker(listing.getNextVersionIdMarker());
                if (verbose) log.info("Successfully got next batch of objects (try #{}).", tries);
                return next;

            } catch (SdkClientException e) {
                lastException = e;
                log.error("Client exception listing objects (try #{}).", tries, e);
            }

            if (tries < maxRetries && Sleep.sleep(50)) break;
        }

        throw new IllegalStateException("Too many errors trying to list objects (maxRetries="+maxRetries+").", lastException);
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
            copy = KeyObjectSummary.S3VersionSummaryToKeyObject(summaries);
            summaries.clear();
        }
        return copy;
    }
}
