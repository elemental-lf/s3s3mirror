package org.cobbzilla.s3s3mirror;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3EncryptionClientBuilder;
import com.amazonaws.services.s3.model.*;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.CmdLineParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.amazonaws.SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Provides the "main" method. Responsible for parsing options and setting up the MirrorMaster to manage the copy.
 */
@Slf4j
public class MirrorMain {

    @Getter @Setter private String[] args;

    @Getter private final MirrorOptions options = new MirrorOptions();

    private final CmdLineParser parser = new CmdLineParser(options);

    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
        @Override public void uncaughtException(Thread t, Throwable e) {
            log.error("Uncaught Exception (thread "+t.getName()+"): "+e, e);
        }
    };

    @Getter private AmazonS3 sourceClient;
    @Getter private AmazonS3 destinationClient;
    @Getter private MirrorContext context;
    @Getter private MirrorMaster master;

    public MirrorMain(String[] args) { this.args = args; }

    public static void main (String[] args) {
        MirrorMain main = new MirrorMain(args);
        main.init();
        main.run();
    }

    public void run() {
        master.mirror();
    }

    public void init() {
        try {
            parseArguments();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.exit(1);
        }

        if (options.isDisableCertCheck())
            System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

        sourceClient = getAmazonS3Client(options.getSourceProfile());
        if (!options.getDestinationProfile().equals(options.getSourceProfile())) {
        	destinationClient = getAmazonS3Client(options.getDestinationProfile());
        } else {
        	destinationClient = sourceClient;
        }

        MirrorEncryption sourceEncryption = options.getSourceProfile().getEncryption();
        MirrorEncryption destinationEncryption = options.getDestinationProfile().getEncryption();
        SSECustomerKey sourceSSEKey = null;
        SSECustomerKey destinationSSEKey = null;

        if (sourceEncryption == MirrorEncryption.SSE_C) {
            sourceSSEKey = new SSECustomerKey(options.getSourceProfile().getEncryptionKey());
        }
        if (destinationEncryption == MirrorEncryption.SSE_C) {
            destinationSSEKey = new SSECustomerKey(options.getDestinationProfile().getEncryptionKey());
        }

        context = new MirrorContext(options, sourceClient, destinationClient, sourceSSEKey, destinationSSEKey);
        master = new MirrorMaster(context);

        Runtime.getRuntime().addShutdownHook(context.getStats().getShutdownHook());
        Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
    }

	protected AmazonS3 getAmazonS3Client(MirrorProfile profile) {
        if (!profile.isValid()) {
            throw new IllegalStateException("Profile is invalid");
        }

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withProtocol(profile.getEndpoint().startsWith("https:") ? Protocol.HTTPS : Protocol.HTTP)
                .withMaxConnections(options.getMaxConnections());

        if (profile.getSignerType() != null) {
            clientConfiguration.setSignerOverride(profile.getSignerType());
        }

        if (profile.hasProxy()) {
            clientConfiguration.setProxyHost(profile.getProxyHost());
            clientConfiguration.setProxyPort(profile.getProxyPort());
        }

        switch (profile.getEncryption()) {
            case CSE_AES_256:
            case CSE_AES_GCM_256:
            case CSE_AES_GCM_256_STRICT:
                CryptoMode cryptoMode = null;

                switch (profile.getEncryption()) {
                    case CSE_AES_256:
                        cryptoMode = CryptoMode.EncryptionOnly;
                        break;
                    case CSE_AES_GCM_256:
                        cryptoMode = CryptoMode.AuthenticatedEncryption;
                        break;
                    case CSE_AES_GCM_256_STRICT:
                        cryptoMode = CryptoMode.StrictAuthenticatedEncryption;
                }

                return AmazonS3EncryptionClientBuilder
                        .standard()
                        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(profile.getEndpoint(), profile.getRegion()))
                        .withPathStyleAccessEnabled(profile.hasOption(MirrorProfileOptions.PATH_STYLE_ACCESS))
                        .withClientConfiguration(clientConfiguration)
                        .withCredentials(new AWSStaticCredentialsProvider(profile))
                        .withCryptoConfiguration(new CryptoConfiguration(cryptoMode))
                        .withEncryptionMaterials(new StaticEncryptionMaterialsProvider(new EncryptionMaterials(profile.getEncryptionKey())))
                        .build();
            default:
                return AmazonS3ClientBuilder
                        .standard()
                        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(profile.getEndpoint(), Regions.US_EAST_1.name()))
                        .withPathStyleAccessEnabled(profile.hasOption(MirrorProfileOptions.PATH_STYLE_ACCESS))
                        .withClientConfiguration(clientConfiguration)
                        .withCredentials(new AWSStaticCredentialsProvider(profile))
                        .build();
        }
    }

    protected void parseArguments() throws Exception {
        parser.parseArgument(args);
        
        if (options.getSourceProfileName() == null || options.getSourceProfileName().equals("")) {
        	throw new IllegalStateException("No source profile specified");
        }

        if (options.getDestinationProfileName() == null || options.getDestinationProfileName().equals("")) {
        	throw new IllegalStateException("No destination profile specified");
        }

        MirrorProfile sourceProfile = options.getSourceProfile();
        MirrorProfile destinationProfile = options.getDestinationProfile();

        sourceProfile.setName(options.getSourceProfileName());
        destinationProfile.setName(options.getDestinationProfileName());

        loadAwsKeysFromS3Config(sourceProfile);
        loadAwsKeysFromS3Config(destinationProfile);

        if (!options.getSourceProfile().isValid()) {
        	throw new IllegalStateException("Could not find source credentials");
        }
        
        if (!options.getDestinationProfile().isValid()) {
        	throw new IllegalStateException("Could not find destination credentials");
        }
        
        options.initDerivedFields();
    }

    // Credit: https://stackoverflow.com/questions/326390/how-do-i-create-a-java-string-from-the-contents-of-a-file
    private static String readFile(String path) throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, UTF_8);
    }

    private void loadAwsKeysFromS3Config(MirrorProfile profile) throws Exception {
        String s3CfgPath = System.getenv("S3CFG");
        if (s3CfgPath == null) {
            s3CfgPath = "./.s3cfg";
            if (!new File(s3CfgPath).isFile())
                s3CfgPath = System.getProperty("user.home") + File.separator + ".s3cfg";
        }

        @Cleanup BufferedReader reader = new BufferedReader(new FileReader(s3CfgPath));
        String line;
        boolean skipSection = true;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.startsWith("[")) {
                if (line.equals("[" + profile.getName() + "]")) {
                    skipSection = false;
                } else {
                    skipSection = true;
                }
                continue;
            }
            if (skipSection) continue;

            if (line.matches("^access_key\\s*=.*")) {
                profile.setAWSAccessKeyId(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^access_key_path\\s*=.*")) {
                String accessKeyPath = line.substring(line.indexOf("=") + 1).trim();
                profile.setAWSAccessKeyId(readFile(accessKeyPath));
            } else if (line.matches("^access_token\\s*=.*")) {
                profile.setAWSSecretKey(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^access_token_path\\s*=.*")) {
                String accessTokenPath = line.substring(line.indexOf("=") + 1).trim();
                profile.setAWSSecretKey(readFile(accessTokenPath));
            } else if (line.matches("^proxy_host\\s*=.*")) {
                profile.setProxyHost(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^proxy_port\\s*=.*")) {
                profile.setProxyPort(Integer.parseInt(line.substring(line.indexOf("=") + 1).trim()));
            } else if (line.matches("^website_endpoint\\s*=.*")) {
                profile.setEndpoint(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^encryption\\s*=.*")) {
                profile.setEncryption(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^encryption_key\\s*=.*")) {
                profile.setEncryptionKey(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^encryption_key_path\\s*=.*")) {
                String encryptionKeyPath = line.substring(line.indexOf("=") + 1).trim();
                profile.setEncryptionKey(readFile(encryptionKeyPath));
            } else if (line.matches("^signer_type\\s*=.*")) {
                profile.setSignerType(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^region\\s*=.*")) {
                profile.setRegion(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^options\\s*=.*")) {
                String[] options = line.substring(line.indexOf("=") + 1).trim().split("\\s*,\\s*");
                for(String option: options) {
                    profile.addOption(MirrorProfileOptions.valueOf(option));
                }
            } else if (line.matches("^\\s*#.*")) {
                continue;
            } else {
                throw new IllegalStateException("unknown line: " + line);
            }
        }
    }
}
