package com.basho.proserv.datamigrator.io;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.Configuration;
import com.basho.riak.client.IRiakObject;

public class RiakObjectBucket implements IRiakObjectWriter, IRiakObjectReader, Iterable<IRiakObject> {
	public static enum BucketMode { READ, WRITE };
	
	private final Logger log = LoggerFactory.getLogger(RiakObjectBucket.class);
	
	private final static int DEFAULT_BUCKET_CHUNK_COUNT = 10000;
	private final static long DEFAULT_BUCKET_CHUNK_MAX_SIZE = 107374182400l;
	private final static String DEFAULT_FILE_PREFIX = "";
	
	private final Configuration config;
	
	private File fileRoot = null;
	private BucketMode bucketMode = null;
	private int bucketChunkSize = DEFAULT_BUCKET_CHUNK_COUNT;
	private long bucketChunkByteSize = DEFAULT_BUCKET_CHUNK_MAX_SIZE;
	private String filePrefix = DEFAULT_FILE_PREFIX;
	
	private Long bucketCount = 0L;
	private Long currentChunkCount = 0L;
	private Long currentChunkByteSize = 0L;
	private Long totalBucketSize = 0L;
	
	private IRiakObjectWriter currentRiakObjectWriter = null;
	private IRiakObjectReader currentRiakObjectReader = null;
	
	private Queue<File> fileQueue = new LinkedBlockingQueue<File>();
	
	public static FilenameFilter dataFileFilter = new FilenameFilter() {
		@Override
		public boolean accept(File dir, String name) {
			return name.toLowerCase().endsWith(".data");
		}
	};
	
	public static FilenameFilter keyFileFilter = new FilenameFilter() {
		@Override
		public boolean accept(File dir, String name) {
			return name.toLowerCase().endsWith(".keys");
		}
	};
	
	public static FilenameFilter loadedKeyFileFilter = new FilenameFilter() {
		@Override
		public boolean accept(File dir, String name) {
			return name.toLowerCase().endsWith(".loadedkeys");
		}
	};
	
	public RiakObjectBucket(File fileRoot, BucketMode bucketMode, Configuration config) {
		this(fileRoot, bucketMode, DEFAULT_BUCKET_CHUNK_COUNT, config);
	}
	
	public RiakObjectBucket(File fileRoot, BucketMode bucketMode, int bucketChunkCount, Configuration config) {
		if (bucketChunkCount < 1) {
			throw new IllegalArgumentException("bucketChunkCount must be greater than 0");
		}
		if (!fileRoot.exists()) {
			fileRoot.mkdir();
		}
		this.fileRoot = fileRoot;
		this.bucketMode = bucketMode;
		this.bucketChunkSize = bucketChunkCount;
		this.config = config;
		
		if (bucketMode == BucketMode.READ) {
			this.populateChunks();
			if (this.dataFilesExist()) {
				if (!this.readNewChunkFile()) {
					throw new IllegalArgumentException("Could not open files for reading");
				}
			} else { 
				log.error("No bucket data files could be found");
			}
		}
	}
	
	public boolean dataFilesExist() {
		String[] fileList = this.fileRoot.list(dataFileFilter);
		return fileList.length > 0;
	}
	
	public void setFilePrefix(String filePrefix) {
		this.filePrefix = filePrefix;
	}
	
	public boolean writeRiakObject(IRiakObject riakObject) {
		if (this.bucketMode == BucketMode.READ) {
			throw new IllegalArgumentException("Bucket is in Read Mode");
		}
		if (riakObject == null) {
			throw new IllegalArgumentException("riakObject cannot be null");
		}
		if (shouldStartNewChunk()) {
			closeChunk();
			writeNewChunkFile();
		}
		this.currentRiakObjectWriter.writeRiakObject(riakObject);
		this.currentChunkByteSize += riakObject.getValue().length;
		++this.currentChunkCount;
		++this.bucketCount;
		return true;
	}
	
	public IRiakObject readRiakObject() {
		if (this.bucketMode == BucketMode.WRITE) {
			throw new IllegalArgumentException("Bucket is in Write Mode");
		}
		
		IRiakObject riakObject = this.currentRiakObjectReader.readRiakObject();
		if (riakObject == null) {
			if (this.readNewChunkFile()) {
				riakObject = this.currentRiakObjectReader.readRiakObject();
			} // else returning null
		}
		if (riakObject != null) {
			this.currentChunkByteSize += riakObject.getValue().length;
		}
		return riakObject; 
	}
		
	public File getFileRoot() {
		return this.fileRoot;
	}
	
	private boolean shouldStartNewChunk() {
		return (this.currentChunkCount >= this.bucketChunkSize ||
				this.currentChunkByteSize >= this.bucketChunkByteSize ||
				this.currentRiakObjectWriter == null);
	}
	
	private void populateChunks() {
		for (String path : this.fileRoot.list(dataFileFilter)) {
			String fullPath = this.fileRoot.getAbsolutePath() + "/" + path;
			File file = new File(fullPath);
			if (!file.isDirectory() && file.isFile()) {
				this.fileQueue.add(file);
			}
		}
	}
	
	private boolean writeNewChunkFile() {
		String filename = this.fileRoot.getAbsolutePath() + "/" 
				+ this.filePrefix + this.bucketCount.toString() + ".data";
		log.debug("Creating new chunk file " + filename);
		this.currentRiakObjectWriter = new ThreadedRiakObjectWriter(new File(filename), config.getQueueSize());
		return true;
	}
	
	private boolean readNewChunkFile() {
		File chunkFile = this.fileQueue.poll();
		if (chunkFile == null) {
			// currentRiakObjectReader will be null if no files in dump/bucket dir
			if (this.currentRiakObjectReader != null) {
				this.currentRiakObjectReader.close();
				this.currentRiakObjectReader = null;
			}
			return false;
		} else {
			this.closeChunk();
			log.debug("Opening chunk file " + chunkFile.getAbsolutePath());
			this.currentRiakObjectReader = new RiakObjectReader(chunkFile, this.config.getResetVClock());
		}
		return true;
	}
	
	private void closeChunk() {
		this.totalBucketSize += this.currentChunkByteSize;
		this.currentChunkByteSize = 0l;
		this.currentChunkCount = 0l;
		if (this.currentRiakObjectReader != null) {
			this.currentRiakObjectReader.close();
			log.debug("Closed chunk file.");
		}
		if (this.currentRiakObjectWriter != null) {
			this.currentRiakObjectWriter.close();
			log.debug("Closed chunk file.");
		}
	}
	
	public void close() {
		this.closeChunk();
	}
	
	public long getBucketSize() {
		return this.totalBucketSize;
	}

	@Override
	public Iterator<IRiakObject> iterator() {
		return new RiakObjectIterator(this);
	}
	
	public Iterable<Key> bucketKeys() {
		return new RiakBucketKeys(this);
	}
	
	private class RiakObjectIterator implements Iterator<IRiakObject> {
		private final RiakObjectBucket riakObjectBucket;
		
		private IRiakObject nextObject = null;
		
		public RiakObjectIterator(RiakObjectBucket riakObjectBucket) {
			this.riakObjectBucket = riakObjectBucket;
			this.nextObject = this.riakObjectBucket.readRiakObject();
		}
		
		@Override
		public boolean hasNext() {
			return nextObject != null;
		}

		@Override
		public IRiakObject next() {
			IRiakObject object = this.nextObject;
			this.nextObject = this.riakObjectBucket.readRiakObject();
			return object;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	private class RiakBucketKeys implements Iterable<Key> {
		private final RiakObjectBucket bucket;
	
		public RiakBucketKeys(RiakObjectBucket bucket) {
			this.bucket = bucket;
		}
		@Override
		public Iterator<Key> iterator() {
			return new RiakBucketKeyIterator(bucket.iterator());
		}
		
	}
	
	private class RiakBucketKeyIterator implements Iterator<Key> {
		Iterator<IRiakObject> objectIterator;
		
		public RiakBucketKeyIterator(Iterator<IRiakObject> objectIterator) {
			this.objectIterator = objectIterator;
		}

		@Override
		public boolean hasNext() {
			return objectIterator.hasNext();
		}

		@Override
		public Key next() {
			IRiakObject object = objectIterator.next();
			return new Key(object.getBucket(), object.getKey());
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
}
