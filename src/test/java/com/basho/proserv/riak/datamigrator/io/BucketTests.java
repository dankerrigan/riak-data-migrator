package com.basho.proserv.riak.datamigrator.io;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.proserv.datamigrator.io.RiakObjectBucket;
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
		
		RiakObjectBucket writeBucket = new RiakObjectBucket(rootFolder, RiakObjectBucket.BucketMode.WRITE, bucketChunkSize);
		
		RiakObject riakObject = new RiakObject(ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8(""));
		
		for (int i = 0; i < this.testRounds; ++i) {
			writeBucket.writeRiakObject(riakObject);
		}
		
		writeBucket.close();
		
		String[] writtenFiles = rootFolder.list();
		assertTrue(writtenFiles.length == testRounds / bucketChunkSize);
	}
	
	@Test
	public void testBucketRead() throws Exception {
		this.testBucketWrite();
		assertTrue(this.rootPath != null);
		
		RiakObjectBucket readBucket = new RiakObjectBucket(this.rootPath, RiakObjectBucket.BucketMode.READ);
		
		int readCount = 0;
		@SuppressWarnings("unused")
		RiakObject riakObject = null;
		while ((riakObject = readBucket.readRiakObject()) != null) {
			++readCount;
		}
		
		assertTrue(readCount == this.testRounds);
	}

}
