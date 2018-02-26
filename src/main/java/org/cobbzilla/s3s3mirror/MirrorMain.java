package org.cobbzilla.s3s3mirror;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;

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

        sourceClient = getAmazonS3Client(options.getSourceCredentials());
        if (!options.getDestinationCredentials().equals(options.getSourceCredentials())) {        	
        	destinationClient = getAmazonS3Client(options.getDestinationCredentials());
        } else {
        	destinationClient = sourceClient;
        }
        context = new MirrorContext(options, sourceClient, destinationClient);
        master = new MirrorMaster(context);

        Runtime.getRuntime().addShutdownHook(context.getStats().getShutdownHook());
        Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
    }

	protected AmazonS3 getAmazonS3Client(MirrorCredentials credentials) {
        ClientConfiguration clientConfiguration = new ClientConfiguration().withProtocol((options.isSsl() ? Protocol.HTTPS : Protocol.HTTP))
                .withMaxConnections(options.getMaxConnections());
        if (options.getHasProxy()) {
            clientConfiguration = clientConfiguration
                    .withProxyHost(options.getProxyHost())
                    .withProxyPort(options.getProxyPort());
        }
              
        if (!credentials.isComplete()) {
            throw new IllegalStateException("No authenication method available");
        }      
             
    	AmazonS3 client = AmazonS3ClientBuilder
				.standard()
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(credentials.getEndpoint(), Regions.US_EAST_1.name()))
				.withPathStyleAccessEnabled(true)
				.withClientConfiguration(clientConfiguration)
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.build();
        
        return client;
    }


    protected void parseArguments() throws Exception {
        parser.parseArgument(args);
        
        if (options.getSourceProfile() == null || options.getSourceProfile().equals("")) {
        	throw new IllegalStateException("No source profile specified");
        }

        if (options.getDestinationProfile() == null || options.getDestinationProfile().equals("")) {
        	throw new IllegalStateException("No destination profile specified");
        }
            
        loadAwsKeysFromS3Config(options.getSourceProfile(), options.getSourceCredentials());
        loadAwsKeysFromS3Config(options.getDestinationProfile(), options.getDestinationCredentials());

        if (!options.getSourceCredentials().isComplete()) {
        	throw new IllegalStateException("Could not find source credentials");
        }
        
        if (!options.getDestinationCredentials().isComplete()) {
        	throw new IllegalStateException("Could not find destination credentials");
        }
        
        options.initDerivedFields();
    }

    private void loadAwsKeysFromS3Config(String profile, MirrorCredentials credentials) throws Exception {
        // try to load from ~/.s3cfg
        @Cleanup BufferedReader reader = new BufferedReader(new FileReader(System.getProperty("user.home")+File.separator+".s3cfg"));
        String line;
        boolean skipSection = true;
        while ((line = reader.readLine()) != null) {
        	line = line.trim();
        	if (line.startsWith("[")) {
        		if (line.equals("[" + profile + "]")) {
        			skipSection = false;
        		} else {
        			skipSection = true;
        		}
        		continue;
        	}
        	if (skipSection) continue;
        	      	
            if (line.startsWith("access_key")) {
                credentials.setAWSAccessKeyId(line.substring(line.indexOf("=") + 1).trim());
            } else if (line.startsWith("access_token")) {
                credentials.setAWSSecretKey(line.substring(line.indexOf("=") + 1).trim());
            } else if (!options.getHasProxy() && line.startsWith("proxy_host")) {
                options.setProxyHost(line.substring(line.indexOf("=") + 1).trim());
            } else if (!options.getHasProxy() && line.startsWith("proxy_port")){
                options.setProxyPort(Integer.parseInt(line.substring(line.indexOf("=") + 1).trim()));
            } else if (line.startsWith("website_endpoint")){
                credentials.setEndpoint(line.substring(line.indexOf("=") + 1).trim());
            }
        }
    }
}
