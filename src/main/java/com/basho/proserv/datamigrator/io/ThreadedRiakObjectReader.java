package com.basho.proserv.datamigrator.io;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.events.RiakObjectEvent;
import com.basho.proserv.datamigrator.util.NamedThreadFactory;

public class ThreadedRiakObjectReader implements IRiakObjectReader {
	@SuppressWarnings("unused")
	private final Logger log = LoggerFactory.getLogger(ThreadedRiakObjectReader.class);
	
	private final boolean resetVClock;
	private final ArrayBlockingQueue<RiakObjectEvent> queue;
	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	private final Future<Runnable> readerFuture;
	
	private static int threadId = 0;
	
	@SuppressWarnings("unused")
	private long count = 0;
	
	@SuppressWarnings("unchecked")
	public ThreadedRiakObjectReader(File file, boolean resetVClock, int queueSize) {
		this.resetVClock = resetVClock;
		this.queue = new ArrayBlockingQueue<RiakObjectEvent>(queueSize);
		
		threadFactory.setNextThreadName(String.format("ThreadedRiakObjectReader-%d", threadId++));
		this.readerFuture = (Future<Runnable>) executor.submit(
				new RiakObjectReaderThread(file, this.queue, this.resetVClock));
	}
	
	
	@Override
	public RiakObjectEvent readRiakObject() {
		RiakObjectEvent riakObject = null;
		try {
			riakObject = queue.take();
			++this.count;
		} catch (InterruptedException e) {
			readerFuture.cancel(true);
			riakObject = null;
		}
		
		if (riakObject != null && riakObject.isStopEvent()) {
			riakObject = null;
		}

		return riakObject;
	}

	@Override
	public void close() {
		this.executor.shutdown();
	}
	
	private class RiakObjectReaderThread  extends RiakObjectReader implements Runnable {
		@SuppressWarnings("unused")
		private final Logger log = LoggerFactory.getLogger(RiakObjectReaderThread.class);
		
		private final ArrayBlockingQueue<RiakObjectEvent> queue;
		
		@SuppressWarnings("unused")
		private long count = 0;
		
		public RiakObjectReaderThread(File file, ArrayBlockingQueue<RiakObjectEvent> queue, boolean resetVClock) {
			super(file, resetVClock);
			this.queue = queue;
		}

		@Override
		public void run() {
			try {
				while (!Thread.currentThread().isInterrupted()) {
					RiakObjectEvent riakObject = super.readRiakObject();
					if (riakObject != null) {
						while (!this.queue.offer(riakObject)) { // offer returns false if op not successful
							Thread.sleep(10);
						}
						++count;
					} else {
						this.queue.put(RiakObjectEvent.STOP);
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
