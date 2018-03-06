package org.cobbzilla.s3s3mirror;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.Date;

import static org.cobbzilla.s3s3mirror.MirrorConstants.GB;

@Slf4j
public class MirrorOptions {

    public static final String S3_PROTOCOL_PREFIX = "s3://";

    public static final String USAGE_SOURCE_PROFILE= "Profile used for source side (from ~/.s3cfg)";
    public static final String OPT_SOURCE_PROFILE= "-Y";
    public static final String LONGOPT_SOURCE_PROFILE = "--source-profile";
    @Option(name=OPT_SOURCE_PROFILE, aliases=LONGOPT_SOURCE_PROFILE, usage=USAGE_SOURCE_PROFILE)
    @Getter @Setter private String sourceProfileName = null;
    
    public static final String USAGE_DESTINATION_PROFILE= "Profile used for destination side (from ~/.s3cfg)";
    public static final String OPT_DESTINATION_PROFILE= "-Z";
    public static final String LONGOPT_DESTINATION_PROFILE = "--destination-profile";
    @Option(name=OPT_DESTINATION_PROFILE, aliases=LONGOPT_DESTINATION_PROFILE, usage=USAGE_DESTINATION_PROFILE)
    @Getter @Setter private String destinationProfileName = null;

    public static final String USAGE_DRY_RUN = "Do not actually do anything, but show what would be done";
    public static final String OPT_DRY_RUN = "-n";
    public static final String LONGOPT_DRY_RUN = "--dry-run";
    @Option(name=OPT_DRY_RUN, aliases=LONGOPT_DRY_RUN, usage=USAGE_DRY_RUN)
    @Getter @Setter private boolean dryRun = false;

    public static final String USAGE_VERBOSE = "Verbose output";
    public static final String OPT_VERBOSE = "-v";
    public static final String LONGOPT_VERBOSE = "--verbose";
    @Option(name=OPT_VERBOSE, aliases=LONGOPT_VERBOSE, usage=USAGE_VERBOSE)
    @Getter @Setter private boolean verbose = false;

    public static final String USAGE_STORAGE_CLASS = "Specify the S3 StorageClass (Standard | ReducedRedundancy)";
    public static final String OPT_STORAGE_CLASS = "-l";
    public static final String LONGOPT_STORAGE_CLASS = "--storage-class";
    @Option(name=OPT_STORAGE_CLASS, aliases=LONGOPT_STORAGE_CLASS, usage=USAGE_STORAGE_CLASS)
    @Getter @Setter private String storageClass = "Standard"; 

    public static final String USAGE_SOURCE_PREFIX = "Only copy objects whose keys start with this prefix";
    public static final String OPT_SOURCE_PREFIX = "-p";
    public static final String LONGOPT_SOURCE_PREFIX = "--source-prefix";
    @Option(name=OPT_SOURCE_PREFIX, aliases=LONGOPT_SOURCE_PREFIX, usage=USAGE_SOURCE_PREFIX)
    @Getter @Setter private String sourcePrefix = null;

    public boolean hasSourcePrefix() { return sourcePrefix != null && sourcePrefix.length() > 0; }
    public int getSourcePrefixLength() { return sourcePrefix == null ? 0 : sourcePrefix.length(); }

    public static final String USAGE_DESTINATION_PREFIX = "Destination prefix (replacing the one specified in --prefix, if any)";
    public static final String OPT_DESTINATION_PREFIX= "-d";
    public static final String LONGOPT_DESTINATION_PREFIX = "--destination-prefix";
    @Option(name=OPT_DESTINATION_PREFIX, aliases=LONGOPT_DESTINATION_PREFIX, usage=USAGE_DESTINATION_PREFIX)
    @Getter @Setter private String destinationPrefix = null;

    public boolean hasDestinationPrefix() { return destinationPrefix != null && destinationPrefix.length() > 0; }
    public int getDestinationPrefixLength() { return destinationPrefix == null ? 0 : destinationPrefix.length(); }

    public static final String USAGE_MAX_CONNECTIONS = "Maximum number of connections to S3 (default 100)";
    public static final String OPT_MAX_CONNECTIONS = "-m";
    public static final String LONGOPT_MAX_CONNECTIONS = "--max-connections";
    @Option(name=OPT_MAX_CONNECTIONS, aliases=LONGOPT_MAX_CONNECTIONS, usage=USAGE_MAX_CONNECTIONS)
    @Getter @Setter private int maxConnections = 100;

    public static final String USAGE_MAX_THREADS = "Maximum number of threads (default 100)";
    public static final String OPT_MAX_THREADS = "-t";
    public static final String LONGOPT_MAX_THREADS = "--max-threads";
    @Option(name=OPT_MAX_THREADS, aliases=LONGOPT_MAX_THREADS, usage=USAGE_MAX_THREADS)
    @Getter @Setter private int maxThreads = 100;

    public static final String USAGE_MAX_RETRIES = "Maximum number of retries for S3 requests (default 5)";
    public static final String OPT_MAX_RETRIES = "-r";
    public static final String LONGOPT_MAX_RETRIES = "--max-retries";
    @Option(name=OPT_MAX_RETRIES, aliases=LONGOPT_MAX_RETRIES, usage=USAGE_MAX_RETRIES)
    @Getter @Setter private int maxRetries = 5;
    
    public static final String USAGE_SIZE_ONLY = "Only use object size when checking for equality and ignore etags";
    public static final String OPT_SIZE_ONLY = "-S";
    public static final String LONGOPT_SIZE_ONLY = "--size-only";
    @Option(name=OPT_SIZE_ONLY, aliases=LONGOPT_SIZE_ONLY, usage=USAGE_SIZE_ONLY)
    @Getter @Setter private boolean sizeOnly = false;

    public static final String USAGE_CTIME = "Only copy objects whose Last-Modified date is younger than this many days. " +
            "For other time units, use these suffixes: y (years), M (months), d (days), w (weeks), h (hours), m (minutes), s (seconds)";
    public static final String OPT_CTIME = "-c";
    public static final String LONGOPT_CTIME = "--ctime";
    @Option(name=OPT_CTIME, aliases=LONGOPT_CTIME, usage=USAGE_CTIME)
    @Getter @Setter private String ctime = null;
    public boolean hasCtime() { return ctime != null; }

    private long initMaxAge() {

        DateTime dateTime = new DateTime(nowTime);

        // all digits -- assume "days"
        if (ctime.matches("^[0-9]+$")) return dateTime.minusDays(Integer.parseInt(ctime)).getMillis();

        // ensure there is at least one digit, and exactly one character suffix, and the suffix is a legal option
        if (!ctime.matches("^[0-9]+[yMwdhms]$")) throw new IllegalArgumentException("Invalid option for ctime: "+ctime);

        if (ctime.endsWith("y")) return dateTime.minusYears(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("M")) return dateTime.minusMonths(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("w")) return dateTime.minusWeeks(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("d")) return dateTime.minusDays(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("h")) return dateTime.minusHours(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("m")) return dateTime.minusMinutes(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("s")) return dateTime.minusSeconds(getCtimeNumber(ctime)).getMillis();
        throw new IllegalArgumentException("Invalid option for ctime: "+ctime);
    }

    private int getCtimeNumber(String ctime) {
        return Integer.parseInt(ctime.substring(0, ctime.length() - 1));
    }

    @Getter private long nowTime = System.currentTimeMillis();
    @Getter private long maxAge;
    @Getter private String maxAgeDate;

    public static final String USAGE_DELETE_REMOVED = "Delete objects from the destination bucket if they do not exist in the source bucket";
    public static final String OPT_DELETE_REMOVED = "-X";
    public static final String LONGOPT_DELETE_REMOVED = "--delete-removed";
    @Option(name=OPT_DELETE_REMOVED, aliases=LONGOPT_DELETE_REMOVED, usage=USAGE_DELETE_REMOVED)
    @Getter @Setter private boolean deleteRemoved = false;

    @Argument(index=0, required=true, usage="Source bucket with optional prefix", metaVar = "<source bucket[/source/prefix]>")
    @Getter @Setter private String sourceBucket;
    @Argument(index=1, required=true, usage="Destination bucket with optional prefix", metaVar = "<source bucket[/source/prefix]>")
    @Getter @Setter private String destinationBucket;

    /**
     * Current max file size allowed in amazon is 5 GB. We can try and provide this as an option too.
     */
    public static final long MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE = 5 * GB;
    private static final long DEFAULT_PART_SIZE = 4 * GB;
    private static final String MULTI_PART_UPLOAD_SIZE_USAGE = "The upload size (in bytes) of each part uploaded as part of a multipart request " +
            "for files that are greater than the max allowed file size of " + MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE + " bytes ("+(MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE/GB)+"GB). " +
            "Defaults to " + DEFAULT_PART_SIZE + " bytes ("+(DEFAULT_PART_SIZE/GB)+"GB).";
    private static final String OPT_MULTI_PART_UPLOAD_SIZE = "-u";
    private static final String LONGOPT_MULTI_PART_UPLOAD_SIZE = "--upload-part-size";
    @Option(name=OPT_MULTI_PART_UPLOAD_SIZE, aliases=LONGOPT_MULTI_PART_UPLOAD_SIZE, usage=MULTI_PART_UPLOAD_SIZE_USAGE)
    @Getter @Setter private long uploadPartSize = DEFAULT_PART_SIZE;

    private static final String CROSS_ACCOUNT_USAGE ="Copy across AWS accounts. Only Resource-based policies are supported (as " +
            "specified by AWS documentation) for cross account copying. " +
            "Default is false (copying within same account, preserving ACLs across copies). " +
            "If this option is active, we give full access to owner of the destination bucket.";
    private static final String OPT_CROSS_ACCOUNT_COPY = "-C";
    private static final String LONGOPT_CROSS_ACCOUNT_COPY = "--cross-account-copy";
    @Option(name=OPT_CROSS_ACCOUNT_COPY, aliases=LONGOPT_CROSS_ACCOUNT_COPY, usage=CROSS_ACCOUNT_USAGE)
    @Getter @Setter private boolean crossAccountCopy = false;
    
    @Getter private MirrorProfile sourceProfile = new MirrorProfile();
    @Getter @Setter private MirrorProfile destinationProfile = new MirrorProfile();

    public static final String USAGE_DISABLE_CERT_CHECK = "Disable checking of TLS certificates";
    public static final String LONGOPT_DISABLE_CERT_CHECK = "--disable-cert-check";
    @Option(name=LONGOPT_DISABLE_CERT_CHECK, usage=USAGE_DISABLE_CERT_CHECK)
    @Getter @Setter private boolean disableCertCheck = false;

    public void initDerivedFields() {

        if (hasCtime()) {
            this.maxAge = initMaxAge();
            this.maxAgeDate = new Date(maxAge).toString();
        }

        String scrubbed;
        int slashPos;

        scrubbed = scrubS3ProtocolPrefix(sourceBucket);
        slashPos = scrubbed.indexOf('/');
        if (slashPos == -1) {
            sourceBucket = scrubbed;
        } else {
            sourceBucket = scrubbed.substring(0, slashPos);
            if (hasSourcePrefix()) throw new IllegalArgumentException("Cannot use a "+OPT_SOURCE_PREFIX+"/"+LONGOPT_SOURCE_PREFIX+" argument and source path that includes a prefix at the same time");
            sourcePrefix = scrubbed.substring(slashPos+1);
        }

        scrubbed = scrubS3ProtocolPrefix(destinationBucket);
        slashPos = scrubbed.indexOf('/');
        if (slashPos == -1) {
            destinationBucket = scrubbed;
        } else {
            destinationBucket = scrubbed.substring(0, slashPos);
            if (hasDestinationPrefix()) throw new IllegalArgumentException("Cannot use a "+OPT_DESTINATION_PREFIX+"/"+LONGOPT_DESTINATION_PREFIX+" argument and destination path that includes a dest-prefix at the same time");
            destinationPrefix = scrubbed.substring(slashPos+1);
        }
    }

    protected String scrubS3ProtocolPrefix(String bucket) {
        bucket = bucket.trim();
        if (bucket.startsWith(S3_PROTOCOL_PREFIX)) {
            bucket = bucket.substring(S3_PROTOCOL_PREFIX.length());
        }
        return bucket;
    }
}
