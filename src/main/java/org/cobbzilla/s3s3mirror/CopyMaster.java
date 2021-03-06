package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class CopyMaster extends KeyMaster {

    public CopyMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        super(context, workQueue, executorService);
    }

    protected AmazonS3 getClient() { return context.getSourceClient(); }
    protected MirrorProfile getProfile(MirrorOptions options) { return context.getOptions().getSourceProfile(); }
    protected String getPrefix(MirrorOptions options) { return options.getSourcePrefix(); }
    protected String getBucket(MirrorOptions options) { return options.getSourceBucket(); }

    protected KeyCopyJob getTask(KeyObjectSummary summary) {
        long maxSingleRequestSize = context.getOptions().getMaxSingleRequestUploadSize();
        if (maxSingleRequestSize != 0 && summary.getSize() > maxSingleRequestSize) {
            return new MultipartKeyCopyJob(context, summary, notifyLock);
        }
        return new KeyCopyJob(context, summary, notifyLock);
    }
}
