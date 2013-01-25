package com.basho.proserv.datamigrator.io;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.util.NamedThreadFactory;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedRiakObjectWriter implements IRiakObjectWriter {
	@SuppressWarnings("unused")
	private final Logger log = LoggerFactory.getLogger(ThreadedRiakObjectReader.class);
	private static final int DEFAULT_QUEUE_SIZE = 10000;
	private static final ByteString STOP_FLAG = ByteString.copyFromUtf8("STOPSTOPSTOP");
	
	private final LinkedBlockingQueue<RiakObject> queue;
	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	private static int threadId = 0;
	private long count = 0;
	
	public ThreadedRiakObjectWriter(File file) {
		this.queue = new LinkedBlockingQueue<RiakObject>(DEFAULT_QUEUE_SIZE);
		this.threadFactory.setNextThreadName(String.format("ThreadedRiakObjectWriter-%d", threadId++));
		executor.submit(new RiakObjectWriterThread(file, queue));
	}
	
	@Override
	public boolean writeRiakObject(RiakObject riakObject) {
		try {
			this.queue.put(riakObject);
		} catch (InterruptedException e) {
			return false;
		}
		return true;
	}

	@Override
	public void close() {
		try {
			this.queue.put(new RiakObject(STOP_FLAG, STOP_FLAG, STOP_FLAG));
		} catch (InterruptedException e) {
			// no-op
		}
		executor.shutdown();
	}
	
	private class RiakObjectWriterThread extends RiakObjectWriter implements Runnable {
		private final LinkedBlockingQueue<RiakObject> queue;
		
		public RiakObjectWriterThread(File file, 
					LinkedBlockingQueue<RiakObject> queue) {
			super(file);
			
			this.queue = queue;
			
		}

		@Override
		public void run() {
			try {
				while (!Thread.currentThread().isInterrupted()) {
					RiakObject riakObject = null;
					while ((riakObject = queue.poll()) == null) {
						Thread.sleep(10);
					}
					if (riakObject.getBucketBS() == STOP_FLAG) {
						break;
					}
					boolean success = super.writeRiakObject(riakObject);
					++count;
				}
			} catch (InterruptedException e) {
				// no-op, allow to exit
			}
			
			super.close();
			
		}
		
	}

}
