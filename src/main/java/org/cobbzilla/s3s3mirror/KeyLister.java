package org.cobbzilla.s3s3mirror;

import java.util.List;

public abstract class KeyLister implements Runnable {
    public abstract boolean isDone();

    @Override
    public abstract void run();

    protected abstract int getSize();

    public abstract List<KeyObjectSummary> getNextBatch();
}
