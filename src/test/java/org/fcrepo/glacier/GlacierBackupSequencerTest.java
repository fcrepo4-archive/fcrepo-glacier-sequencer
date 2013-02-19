package org.fcrepo.glacier;

import static org.mockito.Mockito.*;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFormatException;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.version.VersionException;

import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.sequencer.Sequencer;
import org.modeshape.jcr.api.sequencer.Sequencer.Context;

import org.modeshape.jcr.value.binary.InMemoryBinaryValue;

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.UploadArchiveRequest;
import com.amazonaws.services.glacier.model.UploadArchiveResult;
import com.amazonaws.util.StringInputStream;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class GlacierBackupSequencerTest 
    extends TestCase {
	
	private static String UPLOAD_LOC = "http://localhost:8000/glacier/content";
	private static String UPLOAD_ID = "TJgHcrOSfAkV6hdPqOATYfp_0ZaxL1pIBOc02iZ0gDPMr2ig-nhwd_PafstsdIf6HSrjHnP-3p6LCJClYytFT_CBhT9CwNxbRaM5MetS3I-GqwxI3Y8QtgbJbhEQPs0mJ3KExample";
	private static String DEFAULT_ARCHIVE = "defaultarchive";
    public GlacierBackupSequencerTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( GlacierBackupSequencerTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testSequencerValid()
    {
    	try {
    		UploadArchiveResult upload = mock(UploadArchiveResult.class);
    		when(upload.getArchiveId()).thenReturn(UPLOAD_ID);
    		when(upload.getLocation()).thenReturn(UPLOAD_LOC);
        	AmazonGlacierClient client = mock(AmazonGlacierClient.class);
        	when(client.uploadArchive(any(UploadArchiveRequest.class))).thenReturn(upload);
        	GlacierBackupSequencer test = new GlacierBackupSequencer();
        	test.client = client;
        	Property input = mock(Property.class);
        	InMemoryBinaryValue binary = mock(InMemoryBinaryValue.class);
        	String binString = "the quick brown fox";
        	StringInputStream binValue = new StringInputStream(binString);
        	when(binary.getSize()).thenReturn(Long.valueOf(binString.getBytes().length));
        	when(binary.getStream()).thenReturn(binValue);
            when(input.getName()).thenReturn(JcrConstants.JCR_DATA);
            when(input.getBinary()).thenReturn(binary);
            Node output = mock(Node.class);
            when(output.canAddMixin(GlacierBackupSequencer.GLACIER_BACKUP_MIXIN)).thenReturn(true);
            Context context = mock(Context.class);
            assertTrue(test.execute(input, output, context));
			verify(output).setProperty(GlacierBackupSequencer.GLACIER_ARCHIVE_ID_PROPERTY, UPLOAD_ID);
			verify(output).setProperty(GlacierBackupSequencer.GLACIER_LOCATION_PROPERTY, UPLOAD_LOC);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}
    }
    public void testSequencerInvalid()
    {
    	try {
    		UploadArchiveResult upload = mock(UploadArchiveResult.class);
    		when(upload.getArchiveId()).thenReturn(UPLOAD_ID);
    		when(upload.getLocation()).thenReturn(UPLOAD_LOC);
        	AmazonGlacierClient client = mock(AmazonGlacierClient.class);
        	when(client.uploadArchive(any(UploadArchiveRequest.class))).thenReturn(upload);
        	GlacierBackupSequencer test = new GlacierBackupSequencer();
        	test.client = client;
        	Property input = mock(Property.class);
        	InMemoryBinaryValue binary = mock(InMemoryBinaryValue.class);
        	String binString = "the quick brown fox";
        	StringInputStream binValue = new StringInputStream(binString);
        	when(binary.getSize()).thenReturn(Long.valueOf(binString.getBytes().length));
        	when(binary.getStream()).thenReturn(binValue);
            when(input.getName()).thenReturn(JcrConstants.JCR_PATH);
            when(input.getBinary()).thenReturn(binary);
            Node output = mock(Node.class);
            when(output.canAddMixin(GlacierBackupSequencer.GLACIER_BACKUP_MIXIN)).thenReturn(false);
            Context context = mock(Context.class);
			verify(output, times(0)).setProperty(anyString(), anyString());
            assertFalse(test.execute(input, output, context));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}
    }
}
