package com.basho.proserv.datamigrator;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.io.RiakObjectBucket;
import com.basho.proserv.datamigrator.riak.AbstractClientDataReader;
import com.basho.proserv.datamigrator.riak.ClientReaderFactory;
import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.proserv.datamigrator.riak.ThreadedClientDataReader;
import com.basho.riak.pbc.RiakObject;

// BucketDumper will only work with clients returning protobuffer objects, ie PBClient
public class BucketDumper {
	private final Logger log = LoggerFactory.getLogger(BucketDumper.class);
	private final Connection connection;
	private final File dataRoot;
	private final boolean verboseStatusOutput;
	private int errorCount = 0;
	
	private long timerStart = System.currentTimeMillis();
	private long previousCount = 0;
	
	public BucketDumper(Connection connection, File dataRoot, boolean verboseStatusOutput) {
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
		
		RiakObjectBucket dumpBucket = this.createBucket(bucketName);
				
		try {
			AbstractClientDataReader reader = new ThreadedClientDataReader(connection,
					new ClientReaderFactory(), 
					bucketName, 
					this.connection.riakClient.listKeys(bucketName));
			
			RiakObject riakObject = null;
			while((riakObject = reader.readObject()) != null) {
				dumpBucket.writeRiakObject(riakObject);
				++objectCount;

				if (this.verboseStatusOutput) {
					this.printStatus(objectCount);
				}	
			}
		} catch (IOException e) {
			log.error("Riak error listing keys for bucket: " + bucketName);
			++errorCount;
		} finally {
			dumpBucket.close();
		}
		
		return objectCount;
	}
	
	public int errorCount() {
		return errorCount;
	}
	

	private void printStatus(long objectCount) {
		long end = System.currentTimeMillis();
		if (end-timerStart >= 1000) {
			long total = end-timerStart;
			
			System.out.print("\rRead " + (int)((objectCount-this.previousCount)/(total/1000.0)) + " obj/sec          ");
			System.out.flush();
			
			this.previousCount = objectCount;
			timerStart = System.currentTimeMillis();
		}
	}
	
	private RiakObjectBucket createBucket(String bucketName) {
		String bucketRootPath = this.dataRoot.getAbsolutePath() + "/" + bucketName;
		File bucketRoot = new File(bucketRootPath);
		return new RiakObjectBucket(bucketRoot, RiakObjectBucket.BucketMode.WRITE);
	}
}
