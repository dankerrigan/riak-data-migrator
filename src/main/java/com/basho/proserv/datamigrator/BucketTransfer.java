package com.basho.proserv.datamigrator;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.events.RiakObjectEvent;
import com.basho.proserv.datamigrator.io.IKeyJournal;
import com.basho.proserv.datamigrator.io.KeyJournal;
import com.basho.proserv.datamigrator.riak.ClientReaderFactory;
import com.basho.proserv.datamigrator.riak.ClientWriterFactory;
import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.proserv.datamigrator.riak.ThreadedClientDataReader;
import com.basho.proserv.datamigrator.riak.ThreadedClientDataWriter;

public class BucketTransfer {
	private final Logger log = LoggerFactory.getLogger(BucketLoader.class);
	
	public final Summary summary = new Summary();
	
	private final Configuration configuration;
	
	private final Connection fromConnection;
	private final Connection toConnection;
	
	private long timerStart = System.currentTimeMillis();
	private long previousCount = 0;
	
	@SuppressWarnings("unused")
	private long errorCount = 0;
	
	private boolean verboseStatusOutput = false;
	
	public BucketTransfer(Connection from,  
			  			  Connection to,  
						  Configuration configuration) {
		
		this.configuration = configuration;
		this.fromConnection = from; 
		this.toConnection = to;
		
		this.verboseStatusOutput = this.configuration.getVerboseStatus();
	}

    public long transferKeys(IKeyJournal keyJournal) {
        if (!this.fromConnection.connected()) {
            log.error("Not connected to Riak");
            return 0;
        }
        int objectCount = 0;

        File dataRoot = this.getTemporaryPath(true);
        Map<String, KeyJournal> keyJournals = Utilities.splitKeys(dataRoot, keyJournal);

        for (String bucketName : keyJournals.keySet()) {
            objectCount += transferBucket(bucketName, keyJournals.get(bucketName), false);
        }

        return objectCount;
    }

	public long transferAllBuckets(boolean resume) {
		Set<String> buckets = null;
		if (this.fromConnection.connected()) {
			try {
				buckets = this.fromConnection.riakClient.listBuckets();
			} catch (IOException e) {
				log.error("Riak error listing buckets.", e);
				++this.errorCount;
				return 0;
			}
		} else {
			log.error("Not connected to Riak");
			return 0;
		}
		
		return transferBuckets(buckets, resume);
	}
	
	public long transferBuckets(Set<String> bucketNames, boolean resume) {
		if (!this.fromConnection.connected()) {
			log.error("Not connected to Riak");
			return 0;
		}
		int objectCount = 0;
		for (String bucketName : bucketNames) {
			objectCount += transferBucket(bucketName, resume);
		}
		return objectCount;
	}

    public long transferBucket(String bucketName, boolean resume) {
        File keyPath = getTemporaryPath(false);
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

        KeyJournal bucketKeys = new KeyJournal(keyPath, KeyJournal.Mode.READ);

        return transferBucket(bucketName, bucketKeys, resume);
    }

	public long transferBucket(String bucketName, IKeyJournal keyJournal, boolean resume)  {
		if (bucketName == null || bucketName.isEmpty()) {
			throw new IllegalArgumentException("bucketName cannot be null or empty");
		}
		if (resume) {
			throw new UnsupportedOperationException("Resume is currently unsupported");
		}

		this.previousCount = 0;
		long objectCount = 0;
		long valueErrorCount = 0;
		long dataSize = 0;
		long start = System.currentTimeMillis();
		long keyCount = keyJournal.getKeyCount();
		boolean error = false;

		ClientWriterFactory clientWriterFactory = new ClientWriterFactory();
		clientWriterFactory.setBucketRename(this.configuration.getDestinationBucket());
		
		try {
			// self closing
			ThreadedClientDataReader reader = new ThreadedClientDataReader(fromConnection,
					new ClientReaderFactory(), 
					keyJournal,
					this.configuration.getRiakWorkerCount(),
					this.configuration.getQueueSize());
			
			ThreadedClientDataWriter writer = new ThreadedClientDataWriter(toConnection,
					clientWriterFactory,
					reader,
					this.configuration.getRiakWorkerCount(),
					this.configuration.getQueueSize());

			Event event = null;
			while (!(event = writer.writeObject()).isNullEvent()) {
				if (event.isRiakObjectEvent()) {
					RiakObjectEvent riakEvent = event.asRiakObjectEvent();
					objectCount += riakEvent.count();
					dataSize += riakEvent.dataSize();
				} else if (event.isValueErrorEvent()) { // Count not-founds
					++valueErrorCount;
				} else if (event.isIoErrorEvent()) { // Exit on IOException retry reached
					reader.terminate(); // writer terminates on error coming from reader on error.
					throw new IOException(event.asIoErrorEvent().ioException());
				}

				if (this.verboseStatusOutput) {
					this.printStatus(keyCount, objectCount, false);
				}	
			}
		} catch (IOException e) {
			error = true;
			log.error("Riak error transferring objects for bucket: " + bucketName, e);
			++errorCount;
		}
		
		long stop = System.currentTimeMillis();
		
		if (!error) {
			this.summary.addStatistic(bucketName, 
									  objectCount, 
									  stop-start, 
									  dataSize, 
									  valueErrorCount);
		} else {
			this.summary.addStatistic(bucketName, -1l, 0l, 0l, valueErrorCount);
		}
		
		if (this.verboseStatusOutput) {
			this.printStatus(keyCount, objectCount, true);
		}
		
		return objectCount;
	}
	
	private long dumpBucketKeys(String bucketName, File filePath) throws IOException {
		IKeyJournal keyJournal = new KeyJournal(filePath, KeyJournal.Mode.WRITE);
		long keyCount = 0;
		Iterable<String> keys = this.fromConnection.riakClient.listKeys(bucketName);
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
			int perc = (int)((float)objectCount/(float)keyCount * 100);
			String msg = String.format("\r%d%% completed. Transferred %d @ %d obj/sec          ", perc, objectCount, recsSec);
			System.out.print(msg);
			System.out.flush();
			
			this.previousCount = objectCount;
			this.timerStart = System.currentTimeMillis();
		}
	}
	
}