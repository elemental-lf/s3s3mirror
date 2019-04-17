package org.cobbzilla.s3s3mirror;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Sleep {

    public static boolean sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            log.error("Sleeping for {} milliseconds was interrupted.", millis);
            return true;
        }
        return false;
    }

}
