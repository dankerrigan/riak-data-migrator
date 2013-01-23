package com.basho.proserv.datamigrator.io;

import java.io.File;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.pbc.RiakObject;

public class RiakObjectBucket implements IRiakObjectWriter, IRiakObjectReader, Iterable<RiakObject> {
	public static enum BucketMode { READ, WRITE };
	
	private final Logger log = LoggerFactory.getLogger(RiakObjectBucket.class);
	
	private final static int DEFAULT_BUCKET_CHUNK_COUNT = 10000;
	private final static String DEFAULT_FILE_PREFIX = "";
	
	private File fileRoot = null;
	private BucketMode bucketMode = null;
	private int bucketChunkSize = DEFAULT_BUCKET_CHUNK_COUNT;
	private String filePrefix = DEFAULT_FILE_PREFIX;
	
	private Long bucketCount = 0L;
	
	private IRiakObjectWriter currentRiakObjectWriter = null;
	private IRiakObjectReader currentRiakObjectReader = null;
	
	private Queue<File> fileQueue = new LinkedBlockingQueue<File>();
	
	public RiakObjectBucket(File fileRoot, BucketMode bucketMode) {
		this(fileRoot, bucketMode, DEFAULT_BUCKET_CHUNK_COUNT);
	}
	
	public RiakObjectBucket(File fileRoot, BucketMode bucketMode, int bucketChunkCount) {
		if (bucketChunkCount < 1) {
			throw new IllegalArgumentException("bucketChunkCount must be greater than 0");
		}
		if (!fileRoot.exists()) {
			fileRoot.mkdir();
		}
		this.fileRoot = fileRoot;
		this.bucketMode = bucketMode;
		this.bucketChunkSize = bucketChunkCount;
		
		if (bucketMode == BucketMode.READ) {
			this.populateChunks();
			if (!this.readNewChunkFile()) {
				throw new IllegalArgumentException("Could not open files for reading");
			}
		}
	}
	
	public void setFilePrefix(String filePrefix) {
		this.filePrefix = filePrefix;
	}
	
	public boolean writeRiakObject(RiakObject riakObject) {
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
		++this.bucketCount;
		return true;
	}
	
	public RiakObject readRiakObject() {
		if (this.bucketMode == BucketMode.WRITE) {
			throw new IllegalArgumentException("Bucket is in Write Mode");
		}
		
		RiakObject riakObject = this.currentRiakObjectReader.readRiakObject();
		if (riakObject == null) {
			if (this.readNewChunkFile()) {
				riakObject = this.currentRiakObjectReader.readRiakObject();
			} // else returning null
		}
		
		return riakObject; 
	}
	
	private boolean shouldStartNewChunk() {
		return (bucketCount % this.bucketChunkSize == 0 || 
				this.currentRiakObjectWriter == null);
	}
	
	private void populateChunks() {
		for (String path : this.fileRoot.list()) {
			String fullPath = this.fileRoot.getAbsolutePath() + "/" + path;
			File file = new File(fullPath);
			if (!file.isDirectory() && file.isFile()) {
				this.fileQueue.add(file);
			}
		}
	}
	
	private boolean writeNewChunkFile() {
		String filename = this.fileRoot.getAbsolutePath() + "/" 
				+ this.filePrefix + this.bucketCount.toString();
		log.debug("Creating new chunk file " + filename);
		this.currentRiakObjectWriter = new BatchingRiakObjectWriter(new File(filename));
		return true;
	}
	
	private boolean readNewChunkFile() {
		File chunkFile = this.fileQueue.poll();
		if (chunkFile == null) {
			this.currentRiakObjectReader = null;
			return false;
		} else {
			if (this.currentRiakObjectReader != null) {
				this.currentRiakObjectReader.close();
			}
			log.debug("Opening chunk file " + chunkFile.getAbsolutePath());
			this.currentRiakObjectReader = new ThreadedRiakObjectReader(chunkFile);
		}
		return true;
	}
	
	private void closeChunk() {
		if (this.currentRiakObjectWriter != null) {
			this.currentRiakObjectWriter.close();
			log.debug("Closed chunk file.");
		}
	}
	
	public void close() {
		this.closeChunk();
		if (this.currentRiakObjectReader != null) {
			this.currentRiakObjectReader.close();
		}
	}

	@Override
	public Iterator<RiakObject> iterator() {
		return new RiakObjectIterator(this);
	}
	
	private class RiakObjectIterator implements Iterator<RiakObject> {
		private final RiakObjectBucket riakObjectBucket;
		
		private RiakObject nextObject = null;
		
		public RiakObjectIterator(RiakObjectBucket riakObjectBucket) {
			this.riakObjectBucket = riakObjectBucket;
			this.nextObject = this.riakObjectBucket.readRiakObject();
		}
		
		
		@Override
		public boolean hasNext() {
			return nextObject != null;
		}

		@Override
		public RiakObject next() {
			RiakObject object = this.nextObject;
			this.nextObject = this.riakObjectBucket.readRiakObject();
			return object;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
}
