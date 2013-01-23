package com.basho.proserv.datamigrator;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.io.RiakObjectBucket;
import com.basho.proserv.datamigrator.riak.AbstractClientDataWriter;
import com.basho.proserv.datamigrator.riak.ClientWriterFactory;
import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.proserv.datamigrator.riak.ThreadedClientDataWriter;


// BucketLoader will only work with clients returning protobuffer objects, ie PBClient
public class BucketLoader {
	private final Logger log = LoggerFactory.getLogger(BucketLoader.class);
	private final Connection connection;
	private final File dataRoot;
	private final boolean verboseStatusOutput;
	private int errorCount = 0;
	
	private long timerStart = System.currentTimeMillis();
	private long previousCount = 0;
	
	
	public BucketLoader(Connection connection, File dataRoot, boolean verboseStatusOutput) {
		if (connection == null) {
			throw new IllegalArgumentException("connection cannot be null");
		}
		if (dataRoot == null) {
			throw new IllegalArgumentException("dataRoot cannot be null");
		}
		
		this.connection = connection;
		this.dataRoot = dataRoot;
		this.verboseStatusOutput = verboseStatusOutput;
		
	}
	
	public int LoadAllBuckets() {
		int objectCount = 0;
		
		Set<String> bucketNames = getBucketNames();
		
		objectCount = LoadBuckets(bucketNames);
		
		return objectCount;
	}
	
	public int LoadBuckets(Set<String> buckets) {
		int objectCount = 0;
		for (String bucket : buckets) {
			objectCount += LoadBucket(bucket);
		}
		return objectCount;
	}
	
	public int LoadBucket(String bucketName) {
		if (bucketName == null || bucketName.isEmpty()) {
			throw new IllegalArgumentException("bucketName cannot be null or empty");
		}
		int objectCount = 0;
		
		RiakObjectBucket dumpBucket = this.createBucket(bucketName);
			
		AbstractClientDataWriter writer = 
				new ThreadedClientDataWriter(connection, new ClientWriterFactory(), dumpBucket);
		try {
			while (writer.writeObject()) {
				if (this.verboseStatusOutput) {
					this.printStatus(objectCount);
				}
				++objectCount;
			}
		} catch (IOException e) {
			log.error("Riak error storing value to " + bucketName, e);
			++errorCount;
		}
		return objectCount;
	}
	
	public int errorCount() {
		return errorCount;
	}
	
	public void close() {
		this.connection.close();
	}
	
	private void printStatus(long objectCount) {
		long end = System.currentTimeMillis();
		if (end-timerStart >= 1000) {
			long total = end-timerStart;
			
			System.out.print("\rWrote " + (int)((objectCount-this.previousCount)/(total/1000.0)) + " obj/sec          ");
			System.out.flush();
			
			this.previousCount = objectCount;
			timerStart = System.currentTimeMillis();
		}
	}
	
	private RiakObjectBucket createBucket(String bucketName) {
		String fullPathname = this.dataRoot.getAbsolutePath() + "/" + bucketName;
		File fullPath = new File(fullPathname);
		
		return new RiakObjectBucket(fullPath, RiakObjectBucket.BucketMode.READ);
	}
	
	Set<String> getBucketNames() {
		Set<String> buckets = new HashSet<String>();
		
		for (String bucketName : this.dataRoot.list()) {
			String fullPathname = this.dataRoot.getAbsolutePath() + "/" + bucketName;
			File fullPath = new File(fullPathname);
			if (fullPath.isDirectory()) {
				buckets.add(bucketName);
			}
		}
		
		return buckets;
	}
}

