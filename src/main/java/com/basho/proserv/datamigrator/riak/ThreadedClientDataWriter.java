package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.basho.proserv.datamigrator.util.NamedThreadFactory;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedClientDataWriter extends AbstractClientDataWriter {
//	public enum Status {SUCCESS, STOPPED, ERROR};
	private static final int MAX_QUEUE_SIZE = 10000;
	private static final int WORKER_PROC_MULTIPLER = 2;
	
	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	private final int workerCount;
	private final LinkedBlockingQueue<RiakObject> objectQueue = 
			new LinkedBlockingQueue<RiakObject>(MAX_QUEUE_SIZE);;
	private final LinkedBlockingQueue<RiakObject> returnQueue = 
			new LinkedBlockingQueue<RiakObject>(MAX_QUEUE_SIZE);

	private static String ERROR_STRING= "ERRORERRORERRORERROR";
	private static ByteString ERROR_FLAG = ByteString.copyFromUtf8(ERROR_STRING);
	private static String STOP_STRING = "STOPSTOPSTOPSTOPSTOP";
	private static ByteString STOP_FLAG = ByteString.copyFromUtf8(STOP_STRING);
	
	private final List<Future<Runnable>> threads = new ArrayList<Future<Runnable>>();
	
	private int stoppedCount = 0;
	
	public ThreadedClientDataWriter(Connection connection,
			IClientWriterFactory clientWriterFactory,
			Iterable<RiakObject> objectSource) {
		this(connection, 
			 clientWriterFactory, 
			 objectSource,
			 Runtime.getRuntime().availableProcessors() * WORKER_PROC_MULTIPLER);
	}
	
	ThreadedClientDataWriter(Connection connection,
			IClientWriterFactory clientWriterFactory, 
			Iterable<RiakObject> objectSource, int workerCount) {
		super(connection, clientWriterFactory, objectSource);
		this.workerCount = workerCount;
		
		this.run();
	}
	
	@Override
	public RiakObject writeObject() throws IOException {
		RiakObject riakObject = null;
		try {
			riakObject = this.returnQueue.take();
			// Fast exit if not flag
			if (riakObject.getBucketBS() == ERROR_FLAG || 
					riakObject.getBucketBS() == STOP_FLAG) {
				while (!Thread.interrupted()) {
	//				while ((status = this.returnQueue.poll()) == null) {
	//					Thread.sleep(10);
	//				}
					if (riakObject.getBucketBS() == ERROR_FLAG) {
						this.interruptWorkers();
						throw new IOException();
					} else if (riakObject.getBucketBS() == STOP_FLAG) {
						++this.stoppedCount;
						if (this.stoppedCount == this.workerCount) {
							riakObject = null;
							break;
						}
					} else {
						break;
					}
					riakObject = this.returnQueue.take();
				}
			}
		} catch (InterruptedException e) {
			//success = false;
		} finally {
			this.close();
		}
		
		return riakObject;
	}

	private void interruptWorkers() {
		for (Future<Runnable> future : this.threads) {
			future.cancel(true);
		}
	}
	
	private void close() {
		this.executor.shutdown();
	}

	
	@SuppressWarnings("unchecked")
	private void run() {
		for (Integer i = 0; i < this.workerCount; ++i) {
			this.threadFactory.setNextThreadName(String.format("RiakObjectWriterThread-%d", i));
			this.threads.add((Future<Runnable>) executor.submit(new RiakObjectWriterThread(this.clientWriterFactory.createClientWriter(connection),
					this.objectQueue,
					this.returnQueue)));
		}
		threadFactory.setNextThreadName("RiakObjectProducerThread");
		this.threads.add((Future<Runnable>) this.executor.submit(new RiakObjectProducerThread(this.objectSource,
				this.objectQueue,
				this.workerCount)));
	}
	
	private class RiakObjectProducerThread implements Runnable {
		private final Iterable<RiakObject> riakObjects;
		private final LinkedBlockingQueue<RiakObject> objectQueue;
		private final int stopCount;

		public RiakObjectProducerThread(Iterable<RiakObject> riakObjects,
					LinkedBlockingQueue<RiakObject> objectQueue,
					int stopCount) {
			this.riakObjects = riakObjects;
			this.objectQueue = objectQueue;
			this.stopCount = stopCount;
		}
		
		@Override
		public void run() {
			try {
				for (RiakObject object : this.riakObjects) {
					if (Thread.interrupted()) {
						break;
					}
					while (!objectQueue.offer(object)) {
						Thread.sleep(10);
					}
				}
				for (int i = 0; i < this.stopCount; ++i) {
					this.objectQueue.put(new RiakObject(STOP_FLAG, STOP_FLAG, STOP_FLAG));
				}
			} catch (InterruptedException e) {
				// no-op
			}
		}
		
	}
	
	private class RiakObjectWriterThread implements Runnable {
		
		private final IClientWriter writer;
		private final LinkedBlockingQueue<RiakObject> objectQueue;
		private final LinkedBlockingQueue<RiakObject> returnQueue;
		
		public RiakObjectWriterThread(IClientWriter writer,
					LinkedBlockingQueue<RiakObject> objectQueue,
					LinkedBlockingQueue<RiakObject> returnQueue) {
			this.writer = writer;
			this.objectQueue = objectQueue;
			this.returnQueue = returnQueue;
		}

		@Override
		public void run() {
			try {
				try {
					while (!Thread.interrupted()) {
						RiakObject object = this.objectQueue.take();
						if (object.getBucketBS() == STOP_FLAG) {
							break;
						}
						this.writer.storeRiakObject(object);
						while (!this.returnQueue.offer(object)) {
							Thread.sleep(10);
						}
//						this.returnQueue.put(object);
					}
					this.returnQueue.put(new RiakObject(STOP_FLAG, STOP_FLAG, STOP_FLAG));
				} catch (IOException e) {
					this.returnQueue.put(new RiakObject(ERROR_FLAG, ERROR_FLAG, ERROR_FLAG));
				}
			} catch (InterruptedException e) {
				//no-op
			}
			
		}
		
	}
}
