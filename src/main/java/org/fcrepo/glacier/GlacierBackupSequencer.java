package org.fcrepo.glacier;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import javax.jcr.Binary;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.api.sequencer.Sequencer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.internal.TreeHashInputStream;
import com.amazonaws.services.glacier.model.UploadArchiveRequest;
import com.amazonaws.services.glacier.model.UploadArchiveResult;

public class GlacierBackupSequencer extends Sequencer {
	
	public static String GLACIER_BACKUP_MIXIN = "fcrepo:glacier.backup";
	public static String GLACIER_LOCATION_PROPERTY = "glacier:location";
	public static String GLACIER_VAULT_PROPERTY = "glacier:vault";
	public static String GLACIER_ARCHIVE_ID_PROPERTY = "glacier:archive.id";
	public static String GLACIER_CHECKSUM_PROPERTY = "glacier:checksum";
	
	private static Logger LOG = LoggerFactory.getLogger(GlacierBackupSequencer.class);
	
    AmazonGlacierClient client;
    String defaultVault;
    String accessKey;
    String secretKey;
    String credentials;
    String endpoint;
    
    public GlacierBackupSequencer() {
    }
    

    @Override
    public void initialize( NamespaceRegistry registry,
            NodeTypeManager nodeTypeManager ) throws RepositoryException, IOException {
    	super.initialize(registry, nodeTypeManager);
    	if (credentials != null) {
    		InputStream in = GlacierBackupSequencer.class.getResourceAsStream(credentials);
    		Properties props = new Properties();
    		props.load(in);
    		accessKey = props.getProperty("accessKey");
    		secretKey = props.getProperty("secretKey");
    	}

    	AWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
    	client = new AmazonGlacierClient(creds);
    	client.setEndpoint(endpoint);
    }
    
	@Override
	public boolean execute(Property inputProperty, Node outputNode, Context context)
			throws Exception {
    	LOG.debug("Sequencing property change: \"{}\", expecting \"{}\"", inputProperty.getName(), JcrConstants.JCR_DATA);
        if (JcrConstants.JCR_DATA.equals(inputProperty.getName())) {
        	if (!outputNode.canAddMixin(GLACIER_BACKUP_MIXIN)) {
        		LOG.error("Cannot add mixin \"{}\" to this node", GLACIER_BACKUP_MIXIN);
        		throw new ConstraintViolationException("Cannot add mixin \"" + GLACIER_BACKUP_MIXIN + "\" to this node");
        	}
        	Binary inputBinary = inputProperty.getBinary();
        	InputStream in = inputBinary.getStream();
        	long inputSize = inputBinary.getSize();
        	UploadArchiveRequest request = new UploadArchiveRequest();
        	File outFile = null;
        	
            // read input to get tree hash, cache it in a temporary file
        	TreeHashInputStream treeIn = new TreeHashInputStream(in);
        	byte [] buf = new byte[1024];
        	outFile = File.createTempFile("fcrepo", null);
        	OutputStream out = new FileOutputStream(outFile);
        	int len = -1;
        	long read = 0;
        	while ((len = treeIn.read(buf)) > -1) {
        		out.write(buf,0,len);
        		read += len;
        	}
        	treeIn.close();
        	out.flush();
        	out.close();
        	if (inputSize != read) {
        		LOG.warn("input Binary size did not match bytes read: {} != {}", inputSize, read);
        	}
        	LOG.debug("setting input binary size: {}", read);
        	request.setContentLength(read);
        	
        	String checksum = TreeHashGenerator.calculateTreeHash(treeIn.getChecksums());
        	request.setChecksum(checksum);
        	request.setBody(new FileInputStream(outFile));

        	String vaultName = (outputNode.hasProperty(GLACIER_VAULT_PROPERTY)) ? outputNode.getProperty(GLACIER_VAULT_PROPERTY).getString() : defaultVault;
            request.setVaultName(vaultName);
        	
            request.setContentLength(inputBinary.getSize());
        	//TODO Need to calculate the checksum!
        	request.setChecksum("foo");
        	UploadArchiveResult result = client.uploadArchive(request);
        	outputNode.addMixin(GLACIER_BACKUP_MIXIN);
        	outputNode.setProperty(GLACIER_LOCATION_PROPERTY, result.getLocation());
        	outputNode.setProperty(GLACIER_ARCHIVE_ID_PROPERTY, result.getArchiveId());
        	outputNode.setProperty(GLACIER_CHECKSUM_PROPERTY, checksum);
        	outputNode.setProperty(GLACIER_VAULT_PROPERTY, vaultName);
        	if (outFile != null) {
        		outFile.delete();
        	}
        	LOG.debug("Sequenced output node at path: {}", outputNode.getPath());
        	return true;
        } else {
            return false;
        }
	}
		
}
