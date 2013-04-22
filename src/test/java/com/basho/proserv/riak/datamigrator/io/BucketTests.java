package com.basho.proserv.riak.datamigrator.io;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.proserv.datamigrator.Configuration;
import com.basho.proserv.datamigrator.events.RiakObjectEvent;
import com.basho.proserv.datamigrator.io.RiakObjectBucket;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class BucketTests {
	
	File rootPath = null;
	int testRounds = 100;
	
	@Test
	public void testBucketWrite() throws Exception {
		TemporaryFolder tempFolder = new TemporaryFolder();
		
		int bucketChunkSize = 10;
		
		File rootFolder = tempFolder.newFolder();
		this.rootPath = rootFolder;
		Configuration config = new Configuration();
		
		RiakObjectBucket writeBucket = new RiakObjectBucket(rootFolder, RiakObjectBucket.BucketMode.WRITE, bucketChunkSize, config);
		
		IRiakObject riakObject = ConversionUtilWrapper.convertConcreteToInterface(
				new RiakObject(ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8("")));
		
		RiakObjectEvent event = new RiakObjectEvent(riakObject);
		
		for (int i = 0; i < this.testRounds; ++i) {
			writeBucket.writeRiakObject(event);
		}
		
		writeBucket.close();
		
		String[] writtenFiles = rootFolder.list();
		assertTrue(writtenFiles.length == testRounds / bucketChunkSize);
	}
	
	@Test
	public void testBucketRead() throws Exception {
//		this.testBucketWrite();
		
		TemporaryFolder tempFolder = new TemporaryFolder();
		
		int bucketChunkSize = 10;
		
		File rootFolder = tempFolder.newFolder();
		this.rootPath = rootFolder;
		Configuration config = new Configuration();
		
		RiakObjectBucket writeBucket = new RiakObjectBucket(rootFolder, RiakObjectBucket.BucketMode.WRITE, bucketChunkSize, config);
		
		IRiakObject riakObject = ConversionUtilWrapper.convertConcreteToInterface(
				new RiakObject(ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8("")));

		RiakObjectEvent event = new RiakObjectEvent(riakObject);
		
		for (int i = 0; i < this.testRounds; ++i) {
			writeBucket.writeRiakObject(event);
		}
		
		writeBucket.close();
		
		Thread.sleep(1000); // Wait a bit, let the writing thread complete/close the file
		
		assertTrue(this.rootPath != null);
		
		RiakObjectBucket readBucket = new RiakObjectBucket(this.rootPath, RiakObjectBucket.BucketMode.READ, config);
		
		int readCount = 0;
//		@SuppressWarnings("unused")
//		IRiakObject riakObject = null;
		while ((event = readBucket.readRiakObject()) != null) {
			++readCount;
		}
		
		assertTrue(readCount == this.testRounds);
	}

}
