package com.basho.proserv.datamigrator.io;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.util.NamedThreadFactory;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedRiakObjectReader implements IRiakObjectReader {
	@SuppressWarnings("unused")
	private final Logger log = LoggerFactory.getLogger(ThreadedRiakObjectReader.class);
	private static final int DEFAULT_QUEUE_SIZE = 10000;
	private static final String STOP_STRING = "STOPSTOPSTOPSTOP";
	private static final ByteString STOP_FLAG = ByteString.copyFromUtf8(STOP_STRING);
	private static IRiakObject STOP_OBJECT = ConversionUtilWrapper.convertConcreteToInterface(
			new RiakObject(STOP_FLAG, STOP_FLAG, STOP_FLAG, STOP_FLAG));
	
	private final boolean resetVClock;
	private final LinkedBlockingQueue<IRiakObject> queue;
	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	private final Future<Runnable> readerFuture;
	
	private static int threadId = 0;
	private long count = 0;
	
	@SuppressWarnings("unchecked")
	public ThreadedRiakObjectReader(File file, boolean resetVClock) {
		this.resetVClock = resetVClock;
		this.queue = new LinkedBlockingQueue<IRiakObject>(DEFAULT_QUEUE_SIZE);
		
		threadFactory.setNextThreadName(String.format("ThreadedRiakObjectReader-%d", threadId++));
		this.readerFuture = (Future<Runnable>) executor.submit(
				new RiakObjectReaderThread(file, this.queue, this.resetVClock));
	}
	
	
	@Override
	public IRiakObject readRiakObject() {
		IRiakObject riakObject = null;
		try {
			riakObject = queue.take();
			++this.count;
		} catch (InterruptedException e) {
			readerFuture.cancel(true);
			riakObject = null;
		}
		
		if (riakObject != null && riakObject.getBucket().compareTo(STOP_STRING)==0) {
			riakObject = null;
		}

		return riakObject;
	}

	@Override
	public void close() {
		this.executor.shutdown();
	}
	
	private class RiakObjectReaderThread  extends RiakObjectReader implements Runnable {
		private final Logger log = LoggerFactory.getLogger(RiakObjectReaderThread.class);
		private final LinkedBlockingQueue<IRiakObject> queue;
		private long count = 0;
		
		public RiakObjectReaderThread(File file, LinkedBlockingQueue<IRiakObject> queue, boolean resetVClock) {
			super(file, resetVClock);
			this.queue = queue;
		}

		@Override
		public void run() {
			try {
				while (!Thread.currentThread().isInterrupted()) {
					IRiakObject riakObject = super.readRiakObject();
					if (riakObject != null) {
						while (!this.queue.offer(riakObject)) { // offer returns false if op not successful
							Thread.sleep(10);
						}
						++count;
					} else {
						this.queue.put(STOP_OBJECT);
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
