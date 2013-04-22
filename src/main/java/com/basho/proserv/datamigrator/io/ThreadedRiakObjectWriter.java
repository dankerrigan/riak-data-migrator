package com.basho.proserv.datamigrator.io;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.events.RiakObjectEvent;
import com.basho.proserv.datamigrator.util.NamedThreadFactory;

public class ThreadedRiakObjectWriter implements IRiakObjectWriter {
	@SuppressWarnings("unused")
	private final Logger log = LoggerFactory.getLogger(ThreadedRiakObjectReader.class);
	
	private final ArrayBlockingQueue<RiakObjectEvent> queue;
	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	private static int threadId = 0;
	
	@SuppressWarnings("unused")
	private long count = 0;
		
	public ThreadedRiakObjectWriter(File file, int queueSize) {
		this.queue = new ArrayBlockingQueue<RiakObjectEvent>(queueSize);
		this.threadFactory.setNextThreadName(String.format("ThreadedRiakObjectWriter-%d", threadId++));
		executor.submit(new RiakObjectWriterThread(file, queue));
	}
	
	@Override
	public boolean writeRiakObject(RiakObjectEvent riakObject) {
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
			this.queue.put(RiakObjectEvent.STOP);
		} catch (InterruptedException e) {
			// no-op
		}
		executor.shutdown();
	}
	
	private class RiakObjectWriterThread extends RiakObjectWriter implements Runnable {
		private final ArrayBlockingQueue<RiakObjectEvent> queue;
		
		public RiakObjectWriterThread(File file, 
					ArrayBlockingQueue<RiakObjectEvent> queue) {
			super(file);
			
			this.queue = queue;
			
		}

		@Override
		public void run() {
			try {
				while (!Thread.currentThread().isInterrupted()) {
					RiakObjectEvent riakObject = queue.poll();
					if (riakObject == null) {
						Thread.sleep(10);
						continue;
					}
					if (riakObject.isStopEvent()) {
						break;
					}
					super.writeRiakObject(riakObject);
					++count;
				}
			} catch (InterruptedException e) {
				// no-op, allow to exit
			}
			
			super.close();
			
		}
		
	}

}
