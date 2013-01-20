package com.basho.proserv.datamigrator;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.io.RiakObjectBucket;
import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.pbc.RiakObject;

import static com.basho.riak.client.raw.pbc.ConversionUtilWrapper.convertConcreteToInterface;

// BucketLoader will only work with clients returning protobuffer objects, ie PBClient
public class BucketLoader {
	private final Logger log = LoggerFactory.getLogger(BucketLoader.class);
	private Connection connection = null;
	private File dataRoot = null;
	private int errorCount = 0;
	
	
	public BucketLoader(Connection connection, File dataRoot) {
		if (connection == null) {
			throw new IllegalArgumentException("connection cannot be null");
		}
		if (dataRoot == null) {
			throw new IllegalArgumentException("dataRoot cannot be null");
		}
		
		this.connection = connection;
		this.dataRoot = dataRoot;
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
		
		RawClient riakClient = this.connection.riakClient;

		RiakObjectBucket dumpBucket = this.createBucket(bucketName);
		
		RiakObject riakObject = null;
		while (true) {
			riakObject = dumpBucket.readRiakObject();
			if (riakObject == null) {
				break;
			}
			
			try {
				riakClient.store(convertConcreteToInterface(riakObject));
				++objectCount;
			} catch (IOException e) {
				log.error("Riak error storing value to " + bucketName, e);
				++errorCount;
			}
		}
		
		return objectCount;
	}
	
	public int errorCount() {
		return errorCount;
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

