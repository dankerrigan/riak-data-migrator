package com.basho.riak.pbc;

import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.protobuf.ByteString;

public class RiakObjectIOTests {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();
	
	@Test
	public void testReadWriteRiakObject() throws Exception {
		ByteString vectorClock = ByteString.copyFromUtf8("1234567890");
		ByteString key = ByteString.copyFromUtf8("0987654321");
		ByteString bucket = ByteString.copyFromUtf8("RiakBucket");
		ByteString value = ByteString.copyFromUtf8("Basho4Life");
		
		RiakObject testObject = new RiakObject(vectorClock, bucket, key, value);
		
		File tempFile = tempFolder.newFile();
		
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
		
		RiakObjectIO riakObjectIO = new RiakObjectIO();
		
		riakObjectIO.writeRiakObject(dos, testObject);
		
		dos.flush();
		dos.close();
		
		DataInputStream dis = new DataInputStream(new FileInputStream(tempFile));
		
		RiakObject readObject = riakObjectIO.readRiakObject(dis);

		dis.close();
		
		ByteString readVectorClock = readObject.getVclock();
		ByteString readBucket = readObject.getBucketBS();
		ByteString readKey = readObject.getKeyBS();
		ByteString readValue = readObject.getValue();
		
		assertTrue(readVectorClock.equals(vectorClock));
		assertTrue(readBucket.equals(bucket));
		assertTrue(readKey.equals(key));
		assertTrue(readValue.equals(value));
				
	}

}
