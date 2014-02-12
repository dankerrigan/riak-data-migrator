package com.basho.proserv.datamigrator;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.basho.proserv.datamigrator.io.IKeyJournal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.io.KeyJournal;
import com.basho.proserv.datamigrator.riak.AbstractClientDataDeleter;
import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.proserv.datamigrator.riak.ThreadedClientDataDeleter;

public class BucketDelete {
	private final Logger log = LoggerFactory.getLogger(BucketDelete.class);
	public final Summary summary = new Summary();
	private final Connection connection;
	private final boolean verboseOutput;
	
	private long timerStart = System.currentTimeMillis();
	private long previousCount = 0;
	
	public BucketDelete(Connection connection, boolean verboseOutput) {
		this.connection = connection;
		this.verboseOutput = verboseOutput;
	}

    public long deleteKeys(IKeyJournal keyJournal) {
        if (!this.connection.connected()) {
            log.error("Not connected to Riak");
            return 0;
        }
        int objectCount = 0;

        File dataRoot = this.getTemporaryPath(true);
        if (dataRoot == null) {
            return -1;
        }

        Map<String, KeyJournal> keyJournals = Utilities.splitKeys(dataRoot, keyJournal);

        for (String bucketName : keyJournals.keySet()) {
            objectCount += deleteBucket(bucketName, keyJournals.get(bucketName));
        }

        return objectCount;
    }

	public long deleteBuckets(Set<String> bucketNames) {
		if (bucketNames == null || bucketNames.size() == 0) {
			throw new IllegalArgumentException("bucketNames must not be null and must not be sized 0");
		}
		
		long objectCount = 0;
		
		for (String bucketName : bucketNames) {
			objectCount += this.deleteBucket(bucketName);
		}

		this.connection.close();
		
		return objectCount;
	}

    public long deleteBucket(String bucketName) {
        File keyPath = this.getTemporaryPath(false);
        if (keyPath == null) {
            return -1;
        }

        try {
            dumpBucketKeys(bucketName, keyPath);
        } catch (IOException e) {
            log.error("Error listing keys", e);
            this.summary.addStatistic(bucketName, -2l, 0l, 0l, 0l);
            return -2;
        }

        KeyJournal keys = new KeyJournal(keyPath, KeyJournal.Mode.READ);

        return deleteBucket(bucketName, keys);
    }

	public long deleteBucket(String bucketName, IKeyJournal keyJournal) {
		if (bucketName == null || bucketName.isEmpty()) {
			throw new IllegalArgumentException("bucketName must not be null or empty");
		}
		
		if (this.verboseOutput) {
			System.out.println("\nDumping bucket " + bucketName);
		}
		
		long objectCount = 0;
        long start = System.currentTimeMillis();

        long keyCount = keyJournal.getKeyCount();
		AbstractClientDataDeleter deleter = new ThreadedClientDataDeleter(connection, keyJournal);
		
		try {
			@SuppressWarnings("unused")
			Key key = null;
			while ((key = deleter.deleteObject()) != null) {
				++objectCount;
				
				if (this.verboseOutput) {
					this.printStatus(keyCount, objectCount, false);
				}
			}
		} catch (IOException e) {
			log.error("Error deleting keys");
			this.summary.addStatistic(bucketName, -3l, 0l, 0l, 0l);
			return -3;
		}
		
		long stop = System.currentTimeMillis();
		this.summary.addStatistic(bucketName, objectCount, stop-start, 0l, 0l);
		
		if (this.verboseOutput) {
			this.printStatus(keyCount, objectCount, true);
		}
		
		return objectCount;
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

    private File getTemporaryPath(boolean directory) {
        File keyPath = null;
        try {
            keyPath = File.createTempFile("riak-data-migrator", "bucketName");
            if (directory) {
                keyPath.delete();
                keyPath.mkdir();
            }
			keyPath.deleteOnExit();
		} catch (IOException e){
			log.error("Could not create temporary key list file", e);
		}

        return keyPath;
    }
	
	private void printStatus(long keyCount, long objectCount, boolean force) {
		long end = System.currentTimeMillis();
		if (end-timerStart >= 1000 || force) {
			long total = end-timerStart;
			int recsSec = (int)((objectCount-this.previousCount)/(total/1000.0));
			int perc = (int)((double)objectCount/(double)keyCount * 100);
			String msg = String.format("\r%d%% completed. Deleted %d @ %d obj/sec          ", perc, objectCount, recsSec);
			System.out.print(msg);
			System.out.flush();
			
			this.previousCount = objectCount;
			timerStart = System.currentTimeMillis();
		}
	}
}
