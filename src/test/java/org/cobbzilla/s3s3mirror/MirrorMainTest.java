package org.cobbzilla.s3s3mirror;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import static org.cobbzilla.s3s3mirror.MirrorOptions.*;
import static org.junit.Assert.*;

public class MirrorMainTest {

    public static final String PROFILE = "MirrorMainTest-1";
    public static final String PROFILE_PROXY = "MirrorMainTest-2";
    public static final String SOURCE = "from-bucket";
    public static final String DESTINATION = "to-bucket";

    public static final String STANDARD_ARGUMENTS[] = {LONGOPT_MAX_SINGLE_REQUEST_UPLOAD_SIZE, "123456789", OPT_SOURCE_PROFILE, PROFILE, OPT_DESTINATION_PROFILE, PROFILE_PROXY};

    @Test
    public void testBasicArgs() throws Exception {

        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{SOURCE, DESTINATION}));
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertFalse(options.isDryRun());
        assertEquals(SOURCE, options.getSourceBucket());
        assertEquals(DESTINATION, options.getDestinationBucket());
    }

    @Test
    public void testDryRunArgs() throws Exception {

        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{MirrorOptions.OPT_DRY_RUN, SOURCE, DESTINATION}));
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertTrue(options.isDryRun());
        assertEquals(SOURCE, options.getSourceBucket());
        assertEquals(DESTINATION, options.getDestinationBucket());
    }

    @Test
    public void testMaxConnectionsArgs() throws Exception {

        int maxConns = 42;
        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{MirrorOptions.OPT_MAX_CONNECTIONS, String.valueOf(maxConns), SOURCE, DESTINATION}));
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertFalse(options.isDryRun());
        assertEquals(maxConns, options.getMaxConnections());
        assertEquals(SOURCE, options.getSourceBucket());
        assertEquals(DESTINATION, options.getDestinationBucket());
    }

    @Test
    public void testInlinePrefix() throws Exception {
        final String prefix = "foo";
        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{SOURCE + "/" + prefix, DESTINATION}));
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertEquals(prefix, options.getSourcePrefix());
        assertNull(options.getDestinationPrefix());
    }

    @Test
    public void testInlineDestPrefix() throws Exception {
        final String destPrefix = "foo";
        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{SOURCE, DESTINATION + "/" + destPrefix}));
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertEquals(destPrefix, options.getDestinationPrefix());
        assertNull(options.getSourcePrefix());
    }

    @Test
    public void testInlineSourceAndDestPrefix() throws Exception {
        final String prefix = "foo";
        final String destPrefix = "bar";
        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{SOURCE + "/" + prefix, DESTINATION + "/" + destPrefix}));
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertEquals(prefix, options.getSourcePrefix());
        assertEquals(destPrefix, options.getDestinationPrefix());
    }

    @Test
    public void testInlineSourcePrefixAndPrefixOption() throws Exception {
        final String prefix = "foo";
        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{MirrorOptions.OPT_SOURCE_PREFIX, prefix, SOURCE + "/" + prefix, DESTINATION}));
        try {
            main.parseArguments();
            fail("expected IllegalArgumentException");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testInlineDestinationPrefixAndPrefixOption() throws Exception {
        final String prefix = "foo";
        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{MirrorOptions.OPT_DESTINATION_PREFIX, prefix, SOURCE, DESTINATION + "/" + prefix}));
        try {
            main.parseArguments();
            fail("expected IllegalArgumentException");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testProxyHostAndProxyPortOption() throws Exception {
        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{SOURCE, DESTINATION}));
        main.parseArguments();
        assertEquals("proxy.example.com", main.getOptions().getDestinationProfile().getProxyHost());
        assertEquals(1234, main.getOptions().getDestinationProfile().getProxyPort());
    }

    @Test
    public void testExternalAccessToken() throws Exception {
        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{SOURCE, DESTINATION}));
        main.parseArguments();
        assertEquals("minio123", main.getOptions().getSourceProfile().getAWSSecretKey());
    }

    @Test
    public void testExternalEncryptionKey() throws Exception {
        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{SOURCE, DESTINATION}));
        main.parseArguments();
        assertEquals(MirrorProfile.deriveKey("test789"), main.getOptions().getSourceProfile().getEncryptionKey());
    }

    @Test
    public void testEncryptionOption() throws Exception {
        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{SOURCE, DESTINATION}));
        main.parseArguments();
        assertEquals(MirrorEncryption.CSE_AES_GCM_256_STRICT, main.getOptions().getSourceProfile().getEncryption());
    }

    @Test
    public void testQuirks() throws Exception {
        final MirrorMain main = new MirrorMain(ArrayUtils.addAll(STANDARD_ARGUMENTS, new String[]{SOURCE, DESTINATION}));
        main.parseArguments();
        assertTrue(main.getOptions().getSourceProfile().hasQuirk(MirrorProfileQuirks.NO_ENCODING_TYPE));
        assertFalse(main.getOptions().getSourceProfile().hasQuirk(MirrorProfileQuirks.PATH_STYLE_ACCESS));
    }
}