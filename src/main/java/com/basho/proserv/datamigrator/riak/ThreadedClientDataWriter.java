package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedClientDataWriter extends AbstractClientDataWriter {
	public enum Status {SUCCESS, STOPPED, ERROR};
	private static final int MAX_QUEUE_SIZE = 10000;
	private static final int WORKER_PROC_MULTIPLER = 2;
	
	private final ExecutorService executor = Executors.newCachedThreadPool();
	private final int workerCount;
	private final LinkedBlockingQueue<RiakObject> objectQueue = 
			new LinkedBlockingQueue<RiakObject>(MAX_QUEUE_SIZE);;
	private final LinkedBlockingQueue<Status> returnQueue = 
			new LinkedBlockingQueue<Status>(MAX_QUEUE_SIZE);
	
//	private static ByteString ERROR_FLAG = ByteString.copyFromUtf8("ERRORERRORERROR");
	private static String STOP_STRING = "STOPSTOPSTOPSTOPSTOP";
	private static ByteString STOP_FLAG = ByteString.copyFromUtf8(STOP_STRING);
	
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
	public boolean writeObject() throws IOException {
		boolean success = false;
		try {
			Status status = null;
			while (!Thread.interrupted()) {
//				while ((status = this.returnQueue.poll()) == null) {
//					Thread.sleep(10);
//				}
				status = this.returnQueue.take();
				if (status == Status.ERROR) {
					throw new IOException();
				} else if (status == Status.STOPPED) {
					++this.stoppedCount;
					if (this.stoppedCount == this.workerCount) {
						success = false;
						break;
					}
				} else {
					success = true;
					break;
				}
			}
		} catch (InterruptedException e) {
			//success = false;
		}
		return success;
	}

	@Override
	public void close() throws IOException {
		this.executor.shutdown();
	}

	
	private void run() {
		for (Integer i = 0; i < this.workerCount; ++i) {
			executor.submit(new RiakObjectWriterThread(this.clientWriterFactory.createClientWriter(connection),
					this.objectQueue,
					this.returnQueue));
		}
		this.executor.submit(new RiakObjectProducerThread(this.objectSource,
				this.objectQueue,
				this.workerCount));
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
		private final LinkedBlockingQueue<Status> returnQueue;
		
		public RiakObjectWriterThread(IClientWriter writer,
					LinkedBlockingQueue<RiakObject> objectQueue,
					LinkedBlockingQueue<Status> returnQueue) {
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
						this.returnQueue.put(Status.SUCCESS);
					}
					this.returnQueue.put(Status.STOPPED);
				} catch (IOException e) {
					this.returnQueue.put(Status.ERROR);
				}
			} catch (InterruptedException e) {
				//no-op
			}
			
		}
		
	}
}
