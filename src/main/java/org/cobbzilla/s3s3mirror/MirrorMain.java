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
        main.run();
    }

    public void run() {
        init();
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

        if (sourceEncryption == MirrorEncryption.SSE_C_AES_256) {
            sourceSSEKey = new SSECustomerKey(options.getDestinationProfile().getEncryptionKey());
        }
        if (destinationEncryption == MirrorEncryption.SSE_C_AES_256) {
            destinationSSEKey = new SSECustomerKey(options.getDestinationProfile().getEncryptionKey());
        }

        context = new MirrorContext(options, sourceClient, destinationClient, sourceSSEKey, destinationSSEKey);
        master = new MirrorMaster(context);

        Runtime.getRuntime().addShutdownHook(context.getStats().getShutdownHook());
        Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
    }

	protected AmazonS3 getAmazonS3Client(MirrorProfile profile) {
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withProtocol(profile.getEndpoint().startsWith("https:") ? Protocol.HTTPS : Protocol.HTTP)
                .withMaxConnections(options.getMaxConnections());

        if (profile.getHasProxy()) {
            clientConfiguration = clientConfiguration
                    .withProxyHost(profile.getProxyHost())
                    .withProxyPort(profile.getProxyPort());
        }
              
        if (!profile.isValid()) {
            throw new IllegalStateException("Profile is invalid");
        }

        if (profile.getEncryption() == MirrorEncryption.CSE_AES_GCM_256) {
            return AmazonS3EncryptionClientBuilder
                    .standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(profile.getEndpoint(), Regions.US_EAST_1.name()))
                    .withPathStyleAccessEnabled(true)
                    .withClientConfiguration(clientConfiguration)
                    .withCredentials(new AWSStaticCredentialsProvider(profile))
                    .withCryptoConfiguration(new CryptoConfiguration(CryptoMode.AuthenticatedEncryption))
                    .withEncryptionMaterials(new StaticEncryptionMaterialsProvider(new EncryptionMaterials(profile.getEncryptionKey())))
                    .build();
        } if (profile.getEncryption() == MirrorEncryption.CSE_AES_GCM_256_STRICT) {
            return AmazonS3EncryptionClientBuilder
                    .standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(profile.getEndpoint(), Regions.US_EAST_1.name()))
                    .withPathStyleAccessEnabled(true)
                    .withClientConfiguration(clientConfiguration)
                    .withCredentials(new AWSStaticCredentialsProvider(profile))
                    .withCryptoConfiguration(new CryptoConfiguration(CryptoMode.StrictAuthenticatedEncryption))
                    .withEncryptionMaterials(new StaticEncryptionMaterialsProvider(new EncryptionMaterials(profile.getEncryptionKey())))
                    .build();
        } else {
            return AmazonS3ClientBuilder
                    .standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(profile.getEndpoint(), Regions.US_EAST_1.name()))
                    .withPathStyleAccessEnabled(true)
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

    private void loadAwsKeysFromS3Config(MirrorProfile profile) throws Exception {
        // try to load from ~/.s3cfg
        @Cleanup BufferedReader reader = new BufferedReader(new FileReader(System.getProperty("user.home")+File.separator+".s3cfg"));
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
            } else if (line.matches("^access_token\\s*=.*")) {
                profile.setAWSSecretKey(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^proxy_host\\s*=.*")) {
                profile.setProxyHost(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^proxy_port\\s*=.*")){
                profile.setProxyPort(Integer.parseInt(line.substring(line.indexOf("=") + 1).trim()));
            } else if (line.matches("^website_endpoint\\s*=.*")){
                profile.setEndpoint(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^encryption\\s*=.*")){
                profile.setEncryption(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.matches("^encryption_key\\s*=.*")){
                profile.setEncryptionKey(line.substring(line.indexOf("=") + 1).trim());
            }
        }
    }
}
