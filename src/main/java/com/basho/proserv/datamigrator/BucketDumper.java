package com.basho.proserv.datamigrator;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.io.RiakObjectBucket;
import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;

import static com.basho.riak.client.raw.pbc.ConversionUtilWrapper.convertInterfaceToConcrete;

// BucketDumper will only work with clients returning protobuffer objects, ie PBClient
public class BucketDumper {
	private final Logger log = LoggerFactory.getLogger(BucketDumper.class);
	private Connection connection = null;
	private File dataRoot = null;
	private int errorCount = 0;
	
	public BucketDumper(Connection connection, File dataRoot) {
		if (connection == null) {
			throw new IllegalArgumentException("connection cannot be null");
		}
		if (dataRoot == null) {
			throw new IllegalArgumentException("dataRoot cannot be null");
		}
		
		this.connection = connection;
		this.dataRoot = dataRoot;
	}
	
	public int dumpAllBuckets() {
		Set<String> buckets = null;
		if (this.connection.connected()) {
			try {
				buckets = this.connection.riakClient.listBuckets();
			} catch (IOException e) {
				log.error("Riak error listing buckets.", e);
				++this.errorCount;
				return 0;
			}
		}
		
		return dumpBuckets(buckets);
	}
	
	public int dumpBuckets(Set<String> bucketNames) {
		int objectCount = 0;
		for (String bucketName : bucketNames) {
			objectCount += dumpBucket(bucketName);
		}
		return objectCount;
	}
	
	public int dumpBucket(String bucketName) {
		if (bucketName == null || bucketName.isEmpty()) {
			throw new IllegalArgumentException("bucketName cannot be null or empty");
		}
		int objectCount = 0;
		
		RawClient riakClient = this.connection.riakClient;
		
		RiakObjectBucket dumpBucket = this.createBucket(bucketName);
				
		try {
			for (String key : riakClient.listKeys(bucketName)) {
				RiakResponse resp = riakClient.fetch(bucketName, key);
				for (IRiakObject riakObject : resp.getRiakObjects()) {
					dumpBucket.writeRiakObject(convertInterfaceToConcrete(riakObject));
					++objectCount;
				}
			}
		} catch (IOException e) {
			log.error("Riak error listing keys for bucket: " + bucketName);
			++errorCount;
		}
		
		return objectCount;
	}
	
	public int errorCount() {
		return errorCount;
	}
	
	private RiakObjectBucket createBucket(String bucketName) {
		String bucketRootPath = this.dataRoot.getAbsolutePath() + "/" + bucketName;
		File bucketRoot = new File(bucketRootPath);
		return new RiakObjectBucket(bucketRoot, RiakObjectBucket.BucketMode.WRITE);
	}
}
