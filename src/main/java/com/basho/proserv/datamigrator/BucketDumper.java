package com.basho.proserv.datamigrator;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.events.RiakObjectEvent;
import com.basho.proserv.datamigrator.io.KeyJournal;
import com.basho.proserv.datamigrator.io.RiakObjectBucket;
import com.basho.proserv.datamigrator.riak.AbstractClientDataReader;
import com.basho.proserv.datamigrator.riak.ClientReaderFactory;
import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.proserv.datamigrator.riak.RiakBucketProperties;
import com.basho.proserv.datamigrator.riak.ThreadedClientDataReader;
import com.basho.riak.client.bucket.BucketProperties;

// BucketDumper will only work with clients returning protobuffer objects, ie PBClient
public class BucketDumper {
	private final Logger log = LoggerFactory.getLogger(BucketDumper.class);
	public final Summary summary = new Summary();
	private final Connection connection;
	private final Connection httpConnection;
	private final Configuration config;
	private final File dataRoot;
	private final boolean verboseStatusOutput;
	private int errorCount = 0;
	
	private long timerStart = System.currentTimeMillis();
	private long previousCount = 0;
	
	public BucketDumper(Connection connection, Connection httpConnection, Configuration config) {
		if (connection == null) {
			throw new IllegalArgumentException("connection cannot be null");
		}
		if (config == null) {
			throw new IllegalArgumentException("config cannot be null");
		}
		if (config.getFilePath() == null) {
			throw new IllegalArgumentException("dataRoot cannot be null");
		}
		
		this.connection = connection;
		this.httpConnection = httpConnection;
		this.config = config;
		this.dataRoot = config.getFilePath();
		this.verboseStatusOutput = config.getVerboseStatus();
	}
	
	public long dumpBucketSettings(Set<String> bucketNames) {
		if (bucketNames == null) {
			throw new IllegalArgumentException("bucketNames cannot be null or empty");
		}
		long count = 0;
		for (String bucketName : bucketNames) {
			if (this.verboseStatusOutput) {
				System.out.println("Saving bucket properties for " + bucketName);
			}
			File path = new File(this.createBucketPath(bucketName));
			if (!path.exists()) {
				path.mkdir();
			}
			this.saveBucketSettings(bucketName, path);
			++ count;
		}
		return count;
	}
	
	public long dumpAllBuckets(boolean resume, boolean keysOnly) {
		Set<String> buckets = null;
		if (this.connection.connected()) {
			try {
				buckets = this.connection.riakClient.listBuckets();
			} catch (IOException e) {
				log.error("Riak error listing buckets.", e);
				++this.errorCount;
				return 0;
			}
		} else {
			log.error("Not connected to Riak");
			return 0;
		}
		
		return dumpBuckets(buckets, resume, keysOnly);
	}
	
	public long dumpBuckets(Set<String> bucketNames, boolean resume, boolean keysOnly) {
		if (!this.connection.connected()) {
			log.error("Not connected to Riak");
			return 0;
		}
		int objectCount = 0;
		for (String bucketName : bucketNames) {
			objectCount += dumpBucket(bucketName, resume, keysOnly);
		}
		return objectCount;
	}
	
	// resume is unimplemented
	public long dumpBucket(String bucketName, boolean resume, boolean keysOnly) {
		if (bucketName == null || bucketName.isEmpty()) {
			throw new IllegalArgumentException("bucketName cannot be null or empty");
		}
		if (resume) {
			throw new UnsupportedOperationException("Resume is currently unsupported");
		}

		long start = System.currentTimeMillis();
		
		if (!this.connection.connected()) {
			log.error("Not connected to Riak");
			return 0;
		}
		
		if (this.verboseStatusOutput) {
			System.out.println("\nDumping bucket " + bucketName);
		}
		
		long objectCount = 0;
		long valueErrorCount = 0;
		this.previousCount = 0;
		boolean error = false;

		RiakObjectBucket dumpBucket = this.createBucket(bucketName);

		File keyPath = new File(dumpBucket.getFileRoot().getAbsoluteFile() + "/bucketkeys.keys");
		File dumpedKeyPath = new File(dumpBucket.getFileRoot().getAbsoluteFile() + "/dumpedkeys.keys"); 
		
		long keyCount = 0;
		
		try {
			keyCount = this.dumpBucketKeys(bucketName, keyPath);
		} catch (IOException e){
			log.error("Error listing keys for bucket " + bucketName, e);
			this.summary.addStatistic(bucketName, -2l, 0l, 0l, 0l);
			return 0;
		}
		
		if (keysOnly) {
			String bucketNameKeys= String.format("%s keys", bucketName);
			this.summary.addStatistic(bucketNameKeys, keyCount, System.currentTimeMillis()-start, 0l, 0l);
			return keyCount;
		}
		
		KeyJournal bucketKeys = new KeyJournal(keyPath, KeyJournal.Mode.READ);
				
//		this.saveBucketSettings(bucketName, dumpBucket.getFileRoot());
		
		KeyJournal keyJournal = new KeyJournal(
				dumpedKeyPath, 
				KeyJournal.Mode.WRITE);
		
		try {
			// self closing
			AbstractClientDataReader reader = new ThreadedClientDataReader(connection,
					new ClientReaderFactory(), 
					bucketKeys,
					this.config.getRiakWorkerCount(),
					this.config.getQueueSize());

			Event event = null;
			while (!(event = reader.readObject()).isNullEvent()) {
				if (event.isRiakObjectEvent()) {
					RiakObjectEvent riakEvent = event.asRiakObjectEvent(); 
					dumpBucket.writeRiakObject(riakEvent);
					objectCount += riakEvent.count();
					keyJournal.write(riakEvent.key());
				} else if (event.isValueErrorEvent()) { // Count not-founds
					++valueErrorCount;
				} else if (event.isIoErrorEvent()) { // Exit on IOException retry reached
					throw new IOException(event.asIoErrorEvent().ioException());
				}

				if (this.verboseStatusOutput) {
					this.printStatus(keyCount, objectCount, false);
				}	
			}
		} catch (IOException e) {
			log.error("Riak error dumping objects for bucket: " + bucketName, e);
			error = true;
			++errorCount;
		} catch (InterruptedException e) {
			//no-op
		} finally {
			keyJournal.close();
			dumpBucket.close();
		}
		
		long stop = System.currentTimeMillis();
		
		if (!error) {
			this.summary.addStatistic(bucketName, 
					objectCount, 
					stop-start, 
					dumpBucket.getBucketSize(), 
					valueErrorCount);
		} else {
			this.summary.addStatistic(bucketName, -1l, stop-start, 0l, valueErrorCount);
		}
		
		if (this.verboseStatusOutput) {
			this.printStatus(keyCount, objectCount, true);
		}
		
		return objectCount;
	}
	
	
	public int errorCount() {
		return errorCount;
	}
	
	public long dumpBucketKeys(String bucketName, File filePath) throws IOException {
		KeyJournal keyJournal = new KeyJournal(filePath, KeyJournal.Mode.WRITE);
		long keyCount = 0;
		Iterable<String> keys = this.connection.riakClient.listKeys(bucketName);
		for (String keyString : keys) {
			keyJournal.write(bucketName, keyString);
			++keyCount;
		}
		keyJournal.close();
		return keyCount;
	}
	
	private void saveBucketSettings(String bucketName, File path) {
		File xmlPath = RiakBucketProperties.createBucketSettingsFile(path);
		RiakBucketProperties riakBucketProps = new RiakBucketProperties(this.httpConnection);
		BucketProperties bucketProps = riakBucketProps.getBucketProperties(bucketName);
		boolean success = true;
		if (bucketProps != null) {
			success = riakBucketProps.writeBucketProperties(bucketProps, xmlPath);
			
		} else {
			success = false;
		}
		if (!success) {
			log.error("Could not save bucket properties " + bucketName);
		}
			
	}
	
	private void printStatus(long keyCount, long objectCount, boolean force) {
		long end = System.currentTimeMillis();
		if (end-timerStart >= 1000 || force) {
			long total = end-timerStart;
			int recsSec = (int)((objectCount-this.previousCount)/(total/1000.0));
			int perc = (int)((double)objectCount/(double)keyCount * 100);
			String msg = String.format("\r%d%% completed. Read %d @ %d obj/sec          ", perc, objectCount, recsSec);
			System.out.print(msg);
			System.out.flush();
			
			this.previousCount = objectCount;
			timerStart = System.currentTimeMillis();
		}
	}
	
	private String createBucketPath(String bucketName) {
		String encodedBucketName = Utilities.urlEncode(bucketName);
		return this.dataRoot.getAbsolutePath() + "/" + encodedBucketName;
	}
	
	private RiakObjectBucket createBucket(String bucketName) {
		String bucketRootPath = this.createBucketPath(bucketName);
		File bucketRoot = new File(bucketRootPath);
		return new RiakObjectBucket(bucketRoot, RiakObjectBucket.BucketMode.WRITE, this.config);
	}
}
