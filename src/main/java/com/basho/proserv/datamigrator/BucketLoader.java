package com.basho.proserv.datamigrator;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.io.IKeyJournal;
import com.basho.proserv.datamigrator.io.KeyJournal;
import com.basho.proserv.datamigrator.io.RiakObjectBucket;
import com.basho.proserv.datamigrator.riak.AbstractClientDataWriter;
import com.basho.proserv.datamigrator.riak.ClientWriterFactory;
import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.proserv.datamigrator.riak.RiakBucketProperties;
import com.basho.proserv.datamigrator.riak.ThreadedClientDataWriter;
import com.basho.riak.client.bucket.BucketProperties;


// BucketLoader will only work with clients returning protobuffer objects, ie PBClient
public class BucketLoader {
	private final Logger log = LoggerFactory.getLogger(BucketLoader.class);
	public final Summary summary = new Summary();
	private final Connection connection;
	private final Connection httpConnection;
	private final Configuration config;
	private final File dataRoot;
	private final boolean verboseStatusOutput;
	private int errorCount = 0;
	
	private long timerStart = System.currentTimeMillis();
	private long previousCount = 0;
	
	
	public BucketLoader(Connection connection, Connection httpConnection, Configuration config) {
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
	
	public long loadBucketSettings(Set<String> bucketNames) {
		if (bucketNames == null) {
			throw new IllegalArgumentException("bucketNames cannot be null or empty");
		}
		
		long count = 0;
		for (String bucketName : bucketNames) {
			if (this.verboseStatusOutput) {
				System.out.println("Loading bucket properties for " + bucketName);
			}
			File path = new File(this.createBucketPath(bucketName, true));
			if (!path.exists()) {
				path.mkdir();
			}
			this.restoreBucketSettings(bucketName, path);
			++count;
		}
		
		return count;
	}
	
	public long LoadAllBuckets() {
		long objectCount = 0;
		
		Set<String> bucketNames = getBucketNames();
		
		objectCount = LoadBuckets(bucketNames);
		
		return objectCount;
	}
	
	public long LoadBuckets(Set<String> buckets) {
		long objectCount = 0;
		for (String bucket : buckets) {
			objectCount += LoadBucket(bucket);
		}
		return objectCount;
	}
	
	public long LoadBucket(String bucketName) {
		long start = System.currentTimeMillis();
		if (bucketName == null || bucketName.isEmpty()) {
			throw new IllegalArgumentException("bucketName cannot be null or empty");
		}
		
		if (this.verboseStatusOutput) {
			System.out.println("\nLoading bucket " + bucketName);
		}
		
		long objectCount = 0;
		this.previousCount = 0;
		boolean error = false;
		
		RiakObjectBucket dumpBucket = this.createBucket(bucketName);
		if (!dumpBucket.dataFilesExist()) {
			this.summary.addStatistic(bucketName, -1l, 0l, 0l, 0l);
			if (this.verboseStatusOutput) {
				System.out.println(String.format("No data files found for bucket %s", bucketName));
			}
			return 0;
		}
//		this.restoreBucketSettings(bucketName, dumpBucket.getFileRoot());
		File keyPath = new File(dumpBucket.getFileRoot().getAbsoluteFile() + "/bucketkeys.keys");
		long keyCount = this.scanKeysForBucketSize(keyPath);
		
		ClientWriterFactory clientWriterFactory = new ClientWriterFactory();
		clientWriterFactory.setBucketRename(this.config.getDestinationBucket());
		
		AbstractClientDataWriter writer = 
				new ThreadedClientDataWriter(connection, clientWriterFactory, dumpBucket,
						this.config.getRiakWorkerCount(), this.config.getQueueSize());

		IKeyJournal keyJournal = new KeyJournal(
				KeyJournal.createKeyPathFromPath(new File(this.createBucketPath(bucketName, true) + "/keys" ), true),
					KeyJournal.Mode.WRITE);
		
		try {
			Event event = Event.NULL;
			while (!(event = writer.writeObject()).isNullEvent()) {
				if (this.verboseStatusOutput) {
					this.printStatus(keyCount, objectCount, false);
				}
				
				if (event.isRiakObjectEvent()) {
					keyJournal.write(event.asRiakObjectEvent().key());
					++objectCount;
				} else if (event.isIoErrorEvent()) {
					throw event.asIoErrorEvent().ioException();
				}
				
			}
		} catch (IOException e) {
			log.error("Riak error storing value to " + bucketName, e);
			error = true;
			++errorCount;
		} finally {
			keyJournal.close();
			dumpBucket.close();
		}
	
		long stop = System.currentTimeMillis();
		if (!error) {
			summary.addStatistic(bucketName, objectCount, stop - start, dumpBucket.getBucketSize(), 0l);
		} else {
			this.summary.addStatistic(bucketName, -1l, stop-start, 0l, 0l);
		}
		
		if (this.verboseStatusOutput) {
			this.printStatus(keyCount, objectCount, true);
		}
		return objectCount;
	}
	
	public int errorCount() {
		return errorCount;
	}
	
	public void close() {
		this.connection.close();
	}

	public String createBucketPath(String bucketName, boolean urlEncode) {
		if (urlEncode) {
			bucketName = Utilities.urlEncode(bucketName);
		}
		String fullPathname = this.dataRoot.getAbsolutePath() + "/" + bucketName;
		return fullPathname;
	}
	
	private long scanKeysForBucketSize(File path) {
		long count = 0;
		KeyJournal keyJournal = new KeyJournal(path, KeyJournal.Mode.READ);
		for (@SuppressWarnings("unused") Key key : keyJournal) {
			++count;
		}
		keyJournal.close();
		return count;
	}
	
	private void restoreBucketSettings(String bucketName, File path) {
		File xmlPath = RiakBucketProperties.createBucketSettingsFile(path);
		RiakBucketProperties riakBucketProps = new RiakBucketProperties(this.httpConnection);
		
		BucketProperties bucketProps = riakBucketProps.readBucketProperties(xmlPath);
		
		boolean success = true;
		if (bucketProps != null) {
			success = riakBucketProps.setBucketProperties(bucketName, bucketProps);
			
		} else {
			success = false;
		}
		if (!success) {
			log.error("Could not restore bucket properties " + bucketName);
		}
	}
	
	private void printStatus(long keyCount, long objectCount, boolean force) {
		long end = System.currentTimeMillis();
		if (end-timerStart >= 1000 || force) {
			long total = end-timerStart;
			int recsSec = (int)((objectCount-this.previousCount)/(total/1000.0));
			int perc = (int)((float)objectCount/(float)keyCount * 100);
			String msg = String.format("\r%d%% completed. Wrote %d @ %d obj/sec          ", perc, objectCount, recsSec);
			System.out.print(msg);
			System.out.flush();
			
			this.previousCount = objectCount;
			this.timerStart = System.currentTimeMillis();
		}
	}
	
	private RiakObjectBucket createBucket(String bucketName) {
		String fullPathname = this.createBucketPath(bucketName, true);
		File fullPath = new File(fullPathname);
		
		return new RiakObjectBucket(fullPath, RiakObjectBucket.BucketMode.READ, this.config);
	}
	
	Set<String> getBucketNames() {
		Set<String> buckets = new HashSet<String>();
		
		for (String bucketName : this.dataRoot.list()) {
			String fullPathname = this.createBucketPath(bucketName, false);
			File fullPath = new File(fullPathname);
			if (fullPath.isDirectory()) {
				String decodedBucketName = Utilities.urlDecode(bucketName); 
				buckets.add(decodedBucketName);
			}
		}
		
		return buckets;
	}
}

