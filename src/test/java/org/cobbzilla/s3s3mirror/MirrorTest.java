package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.cobbzilla.s3s3mirror.MirrorOptions.*;
import static org.cobbzilla.s3s3mirror.TestObject.Clean;
import static org.cobbzilla.s3s3mirror.TestObject.Copy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j @RunWith(Parameterized.class)
public class MirrorTest {
    @Parameters(name = "{0}/{1} to {2}/{3} (size {4})")
    public static Collection<Object[]> data() {
        ArrayList<Object[]> list = new ArrayList<Object[]>();

        List<String> sourceProfiles;
        List<String> destinationProfiles;
        String sourceBucket;
        String destinationBucket;
        int uploadPartSize;

        List<String> testProfiles = Arrays.asList("from-google", "to-google");
        List<Integer> sizes = Arrays.asList(12 * 1024, 12 * 1024 * 1024);

        for(String testProfile: testProfiles) {
            if (testProfile.equals("standard")) {
                sourceProfiles = Arrays.asList("MirrorTest-1", "MirrorTest-1-CSE_AES_GCM_256_STRICT", "MirrorTest-1-SSE_C",
                        "MirrorTest-3");
                destinationProfiles = Arrays.asList("MirrorTest-1", "MirrorTest-1-CSE_AES_GCM_256_STRICT", "MirrorTest-1-SSE_C",
                        "MirrorTest-2", "MirrorTest-2-CSE_AES_GCM_256_STRICT", "MirrorTest-2-SSE_C", "MirrorTest-3");

                sourceBucket = "from-bucket";
                destinationBucket = "to-bucket";
                uploadPartSize = 5242880;
            } else if (testProfile.equals("to-google")) {
                sourceProfiles = Arrays.asList("MirrorTest-1");
                destinationProfiles = Arrays.asList("Google");
                sourceBucket = "from-bucket";
                destinationBucket = "umg-de-alpha-s3s3mirror-test";
                uploadPartSize = 0;
            } else if (testProfile.equals("from-google")) {
                sourceProfiles = Arrays.asList("Google");
                destinationProfiles = Arrays.asList("MirrorTest-1");
                sourceBucket = "umg-de-alpha-s3s3mirror-test";
                destinationBucket = "to-bucket";
                uploadPartSize = 0;
            } else {
                throw new IllegalArgumentException("unknown test profile: " + testProfile);
            }

            for (int size : sizes) {
                for (String sourceProfile : sourceProfiles) {
                    for (String destinationProfile : destinationProfiles) {
                        list.add(new Object[]{sourceProfile, sourceBucket,
                                destinationProfile, destinationBucket, size, uploadPartSize});
                    }
                }
            }
        }

        return list;
    }

    @Parameter(0) public String SOURCE_PROFILE = null;
    @Parameter(1) public String SOURCE = null;
    @Parameter(2) public String DESTINATION_PROFILE = null;
    @Parameter(3) public String DESTINATION = null;
    @Parameter(4) public int FILE_SIZE = 0;
    @Parameter(5) public int MULTI_PART_UPLOAD_SIZE = 5242880;

    private String[] getStandardArgs() {
        String args[] = {LONGOPT_DISABLE_CERT_CHECK, OPT_VERBOSE,
                            LONGOPT_MULTI_PART_UPLOAD_SIZE, Integer.toString(MULTI_PART_UPLOAD_SIZE),
                            OPT_SOURCE_PROFILE, SOURCE_PROFILE,
                            OPT_DESTINATION_PROFILE, DESTINATION_PROFILE};
        return args;
    }

    // Every individual test *must* initialize the "main" instance variable, otherwise NPE gets thrown here.
    private MirrorMain main = null;

    private TestObject createTestObject(String key, Copy copy, Clean clean) throws Exception {
        return TestObject.create(main.getSourceClient(), main.getContext().getSourceSSEKey(), SOURCE,
                main.getDestinationClient(), main.getContext().getDestinationSSEKey(), DESTINATION, key, FILE_SIZE, copy, clean);
    }

    private static String random(int size) {
        return RandomStringUtils.randomAlphanumeric(size) + "_" + System.currentTimeMillis();
    }

    private ObjectMetadata getMetadata(AmazonS3 client, SSECustomerKey sseKey, String bucket, String key) {
        GetObjectMetadataRequest getRequest = new GetObjectMetadataRequest(bucket, key)
                .withSSECustomerKey(sseKey);
        return client.getObjectMetadata(getRequest);
    }

    private String getObjectAsString(AmazonS3 client, SSECustomerKey sseKey, String bucket, String key) throws Exception {
        StringWriter writer = new StringWriter();
        GetObjectRequest getRequest = new GetObjectRequest(bucket, key)
                .withSSECustomerKey(sseKey);
        @Cleanup InputStream objectStream = client.getObject(getRequest).getObjectContent();
        IOUtils.copy(objectStream, writer, "UTF-8");
        return writer.toString();
    }

    @After
    public void cleanup () {
        TestObject.cleanupS3Assets();
        main = null;
    }

    @Test
    public void testSimpleCopy () throws Exception {
        final String key = "testSimpleCopy_"+random(10);
        final String[] args = {OPT_SOURCE_PREFIX, key, SOURCE, DESTINATION};

        testSimpleCopyInternal(key, args);
    }

    @Test
    public void testSimpleCopyWithInlinePrefix () throws Exception {
        final String key = "testSimpleCopyWithInlinePrefix_"+random(10);
        final String[] args = {SOURCE + "/" + key, DESTINATION};

        testSimpleCopyInternal(key, args);
    }

    private void testSimpleCopyInternal(String key, String[] args) throws Exception {
        String[] completedArgs = ArrayUtils.addAll(getStandardArgs(), args);
        main = new MirrorMain(completedArgs);
        main.init();
        main.getOptions().setMaxSingleRequestUploadSize(MULTI_PART_UPLOAD_SIZE);

        final TestObject testFile = createTestObject(key, Copy.SOURCE, Clean.SOURCE_AND_DESTINATION);
        log.info("testFile.data.length() " + testFile.data.length());

        main.run();

        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(testFile.data.length(), main.getContext().getStats().bytesCopied.get());

        String object;
        object = getObjectAsString(main.getSourceClient(), main.getContext().getSourceSSEKey(), SOURCE, key);
        assertEquals(testFile.data, object);

        final ObjectMetadata metadata = getMetadata(main.getDestinationClient(), main.getContext().getDestinationSSEKey(), DESTINATION, key);
        assertEquals(testFile.data.length(), KeyJob.getRealObjectSize(metadata));

        object = getObjectAsString(main.getDestinationClient(), main.getContext().getDestinationSSEKey(), DESTINATION, key);
        assertEquals(testFile.data, object);
    }

    @Test
    public void testSimpleCopyWithDestPrefix () throws Exception {
        final String key = "testSimpleCopyWithDestPrefix_"+random(10);
        final String destKey = "dest_testSimpleCopyWithDestPrefix_"+random(10);
        final String[] args = {OPT_SOURCE_PREFIX, key, OPT_DESTINATION_PREFIX, destKey, SOURCE, DESTINATION};
        testSimpleCopyWithDestPrefixInternal(key, destKey, args);
    }

    @Test
    public void testSimpleCopyWithInlineDestPrefix () throws Exception {
        final String key = "testSimpleCopyWithInlineDestPrefix_"+random(10);
        final String destKey = "dest_testSimpleCopyWithInlineDestPrefix_"+random(10);
        final String[] args = {SOURCE+"/"+key, DESTINATION+"/"+destKey };
        testSimpleCopyWithDestPrefixInternal(key, destKey, args);
    }

    private void testSimpleCopyWithDestPrefixInternal(String key, String destKey, String[] args) throws Exception {
        String[] completedArgs = ArrayUtils.addAll(getStandardArgs(), args);
        main = new MirrorMain(completedArgs);
        main.init();
        main.getOptions().setMaxSingleRequestUploadSize(MULTI_PART_UPLOAD_SIZE);

        final TestObject testFile = createTestObject(key, Copy.SOURCE, Clean.SOURCE_AND_DESTINATION);

        main.run();

        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(testFile.data.length(), main.getContext().getStats().bytesCopied.get());

        String object;
        object = getObjectAsString(main.getSourceClient(), main.getContext().getSourceSSEKey(), SOURCE, key);
        assertEquals(testFile.data, object);

        final ObjectMetadata metadata = getMetadata(main.getDestinationClient(), main.getContext().getDestinationSSEKey(), DESTINATION, destKey);
        assertEquals(testFile.data.length(), KeyJob.getRealObjectSize(metadata));

        object = getObjectAsString(main.getDestinationClient(), main.getContext().getDestinationSSEKey(), DESTINATION, destKey);
        assertEquals(testFile.data, object);
    }

    @Test
    public void testDeleteRemoved () throws Exception {
        final String key = "testDeleteRemoved_"+random(10);

        final String[] args = ArrayUtils.addAll(getStandardArgs(), new String[] {OPT_SOURCE_PREFIX, key,
                OPT_DELETE_REMOVED, SOURCE, DESTINATION});
        main = new MirrorMain(args);
        main.init();
        main.getOptions().setMaxSingleRequestUploadSize(MULTI_PART_UPLOAD_SIZE);

        // Write some files to dest
        final int numDestFiles = 3;
        final String[] destKeys = new String[numDestFiles];
        final TestObject[] destFiles = new TestObject[numDestFiles];
        for (int i=0; i<numDestFiles; i++) {
            destKeys[i] = key + "-dest" + i;
            destFiles[i] = createTestObject(destKeys[i], Copy.DESTINATION, Clean.DESTINATION);
        }

        // Write 1 file to source
        final String srcKey = key + "-src";
        final TestObject srcFile = createTestObject(srcKey, Copy.SOURCE, Clean.SOURCE_AND_DESTINATION);

        // Initiate copy
        main.run();

        // Expect only 1 copy and numDestFiles deletes
        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(numDestFiles, main.getContext().getStats().objectsDeleted.get());

        // Expect none of the original dest files to be there anymore
        for (int i=0; i<numDestFiles; i++) {
            try {
                main.getDestinationClient().getObjectMetadata(DESTINATION, destKeys[i]);
                fail("testDeleteRemoved: expected "+destKeys[i]+" to be removed from destination bucket "+DESTINATION);
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() != 404) {
                    fail("testDeleteRemoved: unexpected exception (expected statusCode == 404): "+e);
                }
            }
        }

        // Expect source file to now be present in both source and destination buckets
        ObjectMetadata metadata;
        metadata = getMetadata(main.getSourceClient(), main.getContext().getSourceSSEKey(), SOURCE, srcKey);
        assertEquals(srcFile.data.length(), KeyJob.getRealObjectSize(metadata));

        String object;
        object = getObjectAsString(main.getSourceClient(), main.getContext().getSourceSSEKey(), SOURCE, srcKey);
        assertEquals(srcFile.data, object);

        metadata = getMetadata(main.getDestinationClient(), main.getContext().getDestinationSSEKey(), DESTINATION, srcKey);
        assertEquals(srcFile.data.length(), KeyJob.getRealObjectSize(metadata));

        object = getObjectAsString(main.getDestinationClient(), main.getContext().getDestinationSSEKey(), DESTINATION, srcKey);
        assertEquals(srcFile.data, object);
    }

}
