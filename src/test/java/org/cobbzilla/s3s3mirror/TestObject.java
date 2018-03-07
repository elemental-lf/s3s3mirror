package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSECustomerKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

@Slf4j
class TestObject {
    enum Copy  { SOURCE, DESTINATION, SOURCE_AND_DESTINATION }
    enum Clean { SOURCE, DESTINATION, SOURCE_AND_DESTINATION }

    public File file;
    public String data;

    private static List<S3Asset> stuffToCleanup = new ArrayList<S3Asset>();

    private static String random(int size) {
        return RandomStringUtils.randomAlphanumeric(size) + "_" + System.currentTimeMillis();
    }

    public TestObject(int fileSize) throws Exception {
        file = File.createTempFile(getClass().getName(), ".tmp");
        data = random(fileSize + (RandomUtils.nextInt() % 1024));
        @Cleanup FileOutputStream out = new FileOutputStream(file);
        IOUtils.copy(new ByteArrayInputStream(data.getBytes()), out);
        file.deleteOnExit();
    }

    public static void cleanupS3Assets () {
        for (S3Asset asset : stuffToCleanup) {
            try {
                log.info("cleanupS3Assets: deleting "+asset);
                asset.getClient().deleteObject(asset.getBucket(), asset.getKey());
            } catch (Exception e) {
                log.error("Error cleaning up object: "+asset+": "+e.getMessage());
            }
        }
        stuffToCleanup.clear();
    }

    public static TestObject create(AmazonS3 sourceClient, SSECustomerKey sourceKey, String sourceBucket,
                                    AmazonS3 destinationClient, SSECustomerKey destinationKey, String destinationBucket,
                                    String key, int fileSize, Copy copy, Clean clean) throws Exception {
        TestObject testFile = new TestObject(fileSize);
        switch (clean) {
            case SOURCE:
                stuffToCleanup.add(new S3Asset(sourceClient, sourceBucket, key));
                break;
            case DESTINATION:
                stuffToCleanup.add(new S3Asset(destinationClient, destinationBucket, key));
                break;
            case SOURCE_AND_DESTINATION:
                stuffToCleanup.add(new S3Asset(sourceClient,sourceBucket, key));
                stuffToCleanup.add(new S3Asset(destinationClient,destinationBucket, key));
                break;
        }
        PutObjectRequest putObject;
        switch (copy) {
            case SOURCE:
                putObject = new PutObjectRequest(sourceBucket, key, testFile.file)
                        .withSSECustomerKey(sourceKey);
                sourceClient.putObject(putObject);
                break;
            case DESTINATION:
                putObject = new PutObjectRequest(destinationBucket, key, testFile.file)
                        .withSSECustomerKey(destinationKey);
                destinationClient.putObject(putObject);
                break;
            case SOURCE_AND_DESTINATION:
                putObject = new PutObjectRequest(sourceBucket, key, testFile.file)
                        .withSSECustomerKey(sourceKey);
                sourceClient.putObject(putObject);
                putObject = new PutObjectRequest(destinationBucket, key, testFile.file)
                        .withSSECustomerKey(destinationKey);
                destinationClient.putObject(putObject);
                break;
        }
        return testFile;
    }
}
