package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class DeleteMaster extends KeyMaster {

    public DeleteMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        super(context, workQueue, executorService);
    }

    protected AmazonS3 getClient() { return context.getDestinationClient(); }
    protected MirrorProfile getProfile(MirrorOptions options) { return context.getOptions().getDestinationProfile(); }

    protected String getPrefix(MirrorOptions options) {
        return options.hasDestinationPrefix() ? options.getDestinationPrefix() : options.getSourcePrefix();
    }

    protected String getBucket(MirrorOptions options) { return options.getDestinationBucket(); }

    @Override
    protected KeyJob getTask(KeyObjectSummary summary) {
        return new KeyDeleteJob(context, summary, notifyLock);
    }
}
