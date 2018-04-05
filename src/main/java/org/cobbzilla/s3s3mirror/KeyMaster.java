package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public abstract class KeyMaster implements Runnable {

    public static final int STOP_TIMEOUT_SECONDS = 10;
    private static final long STOP_TIMEOUT = TimeUnit.SECONDS.toMillis(STOP_TIMEOUT_SECONDS);

    protected MirrorContext context;

    private AtomicBoolean done = new AtomicBoolean(false);
    public boolean isDone () { return done.get(); }

    private BlockingQueue<Runnable> workQueue;
    private ThreadPoolExecutor executorService;
    protected final Object notifyLock = new Object();

    private Thread thread;

    public KeyMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        this.context = context;
        this.workQueue = workQueue;
        this.executorService = executorService;
    }

    protected abstract AmazonS3 getClient();
    protected abstract MirrorProfile getProfile(MirrorOptions options);
    protected abstract String getPrefix(MirrorOptions options);
    protected abstract String getBucket(MirrorOptions options);

    protected abstract KeyJob getTask(KeyObjectSummary summary);

    public void start () {
        this.thread = new Thread(this);
        this.thread.start();
    }

    @SuppressWarnings("deprecation")
	public void stop () {
        final String name = getClass().getSimpleName();
        final long start = System.currentTimeMillis();
        log.info("stopping "+ name +"...");
        try {
            if (isDone()) return;
            this.thread.interrupt();
            while (!isDone() && System.currentTimeMillis() - start < STOP_TIMEOUT) {
                if (Sleep.sleep(50)) return;
            }
        } finally {
            if (!isDone()) {
                try {
                    log.warn(name+" didn't stop within "+STOP_TIMEOUT_SECONDS+" after interrupting it, forcibly killing the thread...");
                    this.thread.stop();
                } catch (Exception e) {
                    log.error("Error calling Thread.stop on " + name + ": " + e, e);
                }
            }
            if (isDone()) log.info(name+" stopped");
        }
    }

    public void run() {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();

        final int maxQueueCapacity = MirrorMaster.getMaxQueueCapacity(options);

        int counter = 0;
        try {
            boolean useKeyVersionLister = false;

            if (false) {
                try {
                    BucketVersioningConfiguration versioning = getClient().getBucketVersioningConfiguration(getBucket(options));

                    if (versioning.getStatus().equals("Enabled")) {
                        useKeyVersionLister = true;
                        if (verbose) log.info("Using KeyVersionLister for " + getProfile(options).getEndpoint() + "/" + getBucket(options));
                    } else {
                        if (verbose) log.info("BucketVersioningConfiguration for " + getProfile(options).getEndpoint()
                                + "/" + getBucket(options) + " is " + versioning.getStatus());
                    }
                } catch (AmazonS3Exception e) {
                    log.error("getBucketVersioningConfiguration failed: " + e);
                    useKeyVersionLister = false;
                }
            }

            KeyLister lister;
            if (useKeyVersionLister) {
                lister = new KeyVersionLister(context, maxQueueCapacity, getProfile(options), getClient(), getBucket(options), getPrefix(options));
            } else {
                lister = new KeyObjectLister(context, maxQueueCapacity, getProfile(options), getClient(), getBucket(options), getPrefix(options));
            }
            executorService.submit(lister);

            List<KeyObjectSummary> summaries = lister.getNextBatch();
            if (verbose) log.info(summaries.size()+" keys found in first batch from bucket -- processing...");

            while (true) {
                for (KeyObjectSummary summary : summaries) {
                    while (workQueue.size() >= maxQueueCapacity) {
                        try {
                            synchronized (notifyLock) {
                                notifyLock.wait(50);
                            }
                            Thread.sleep(50);

                        } catch (InterruptedException e) {
                            log.error("interrupted!");
                            return;
                        }
                    }
                    executorService.submit(getTask(summary));
                    counter++;
                }

                summaries = lister.getNextBatch();
                if (summaries.size() > 0) {
                    if (verbose) log.info(summaries.size()+" more keys found in bucket -- continuing (queue size="+workQueue.size()+", total processed="+counter+")...");

                } else if (lister.isDone()) {
                    if (verbose) log.info("No more keys found in source bucket -- ALL DONE");
                    return;

                } else {
                    if (verbose) log.info("Lister has no keys queued, but is not done, waiting and retrying");
                    if (Sleep.sleep(50)) return;
                }
            }

        } catch (Exception e) {
            log.error("Unexpected exception in MirrorMaster: "+e, e);

        } finally {
            while (workQueue.size() > 0 || executorService.getActiveCount() > 0) {
                // wait for the queue to be empty
                if (Sleep.sleep(100)) break;
            }
            // this will wait for currently executing tasks to finish
            executorService.shutdown();
            done.set(true);
        }
    }
}
