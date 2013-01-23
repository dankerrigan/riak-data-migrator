package com.basho.proserv.datamigrator.io;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedRiakObjectReader implements IRiakObjectReader {
	@SuppressWarnings("unused")
	private final Logger log = LoggerFactory.getLogger(ThreadedRiakObjectReader.class);
	private static final int DEFAULT_QUEUE_SIZE = 10000;
	private static final ByteString STOP_FLAG = ByteString.copyFromUtf8("STOPSTOPSTOP");
	
	private final LinkedBlockingQueue<RiakObject> queue;
	
	private long count = 0;
	
	public ThreadedRiakObjectReader(File file) {
		this.queue = new LinkedBlockingQueue<RiakObject>(DEFAULT_QUEUE_SIZE);
		
		Thread thread = new Thread(new RiakObjectReaderThread(file, this.queue));
		thread.setName(ThreadedRiakObjectReader.class.getName());
		thread.setDaemon(true);
		thread.start();
	}
	
	
	@Override
	public RiakObject readRiakObject() {
		RiakObject riakObject = null;
		try {
			riakObject = queue.take();
			++this.count;
		} catch (InterruptedException e) {
			riakObject = null;
		}
		
		if (riakObject != null && riakObject.getBucketBS() == STOP_FLAG) {
			riakObject = null;
		}

		return riakObject;
	}

	@Override
	public void close() {
		//no-op
		
	}
	
	private class RiakObjectReaderThread extends RiakObjectReader implements Runnable {
		private final Logger log = LoggerFactory.getLogger(RiakObjectReaderThread.class);
		private final LinkedBlockingQueue<RiakObject> queue;
		private long count = 0;
		
		public RiakObjectReaderThread(File file, LinkedBlockingQueue<RiakObject> queue) {
			super(file);
			this.queue = queue;
		}

		@Override
		public void run() {
			try {
				while (!Thread.currentThread().isInterrupted()) {
					RiakObject riakObject = super.readRiakObject();
					if (riakObject != null) {
						while (!this.queue.offer(riakObject)) { // offer returns false if op not successful
							Thread.sleep(100);
						}
						++count;
					} else {
						this.queue.put(new RiakObject(STOP_FLAG,
													  STOP_FLAG,
													  STOP_FLAG));
						break;
					}
				}
			} catch (InterruptedException e) {
				// no-op, allow to exit
			}
			
			super.close();
		}
	}

}
