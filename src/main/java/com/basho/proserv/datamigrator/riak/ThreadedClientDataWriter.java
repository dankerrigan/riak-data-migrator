package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.util.NamedThreadFactory;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedClientDataWriter extends AbstractClientDataWriter {
	private final Logger log = LoggerFactory.getLogger(ThreadedClientDataWriter.class);
//	public enum Status {SUCCESS, STOPPED, ERROR};
	private static final int MAX_QUEUE_SIZE = 10000;
	private static final int WORKER_PROC_MULTIPLER = 2;
	
	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	private final int workerCount;
	private final ArrayBlockingQueue<IRiakObject> objectQueue = 
			new ArrayBlockingQueue<IRiakObject>(MAX_QUEUE_SIZE);
	private final ArrayBlockingQueue<IRiakObject> returnQueue = 
			new ArrayBlockingQueue<IRiakObject>(MAX_QUEUE_SIZE);

	private static String ERROR_STRING= "ERRORERRORERRORERROR";
	private static ByteString ERROR_FLAG = ByteString.copyFromUtf8(ERROR_STRING);
	private static String STOP_STRING = "STOPSTOPSTOPSTOPSTOP";
	private static ByteString STOP_FLAG = ByteString.copyFromUtf8(STOP_STRING);
	private static IRiakObject STOP_OBJECT = ConversionUtilWrapper.convertConcreteToInterface(
			new RiakObject(STOP_FLAG, STOP_FLAG, STOP_FLAG, STOP_FLAG));
	private static IRiakObject ERROR_OBJECT = ConversionUtilWrapper.convertConcreteToInterface(
			new RiakObject(ERROR_FLAG, ERROR_FLAG, ERROR_FLAG, ERROR_FLAG));
	
	private final List<Future<Runnable>> threads = new ArrayList<Future<Runnable>>();
	
	private int stoppedCount = 0;
	
	public ThreadedClientDataWriter(Connection connection,
			IClientWriterFactory clientWriterFactory,
			Iterable<IRiakObject> objectSource) {
		this(connection, 
			 clientWriterFactory, 
			 objectSource,
			 Runtime.getRuntime().availableProcessors() * WORKER_PROC_MULTIPLER);
	}
	
	public ThreadedClientDataWriter(Connection connection,
			IClientWriterFactory clientWriterFactory, 
			Iterable<IRiakObject> objectSource, 
			int workerCount) {
		super(connection, clientWriterFactory, objectSource);
		this.workerCount = workerCount;
		
		this.run();
	}
	
	
	public IRiakObject writeObject() throws IOException {
		IRiakObject riakObject = null;
		try {
			riakObject = this.returnQueue.take();
			
			// Fast exit if not flag
			if (isError(riakObject) || 
					isStop(riakObject)) {
				while (!Thread.interrupted()) {
	//				while ((status = this.returnQueue.poll()) == null) {
	//					Thread.sleep(10);
	//				}
					if (isError(riakObject)) {
						this.interruptWorkers();
						throw new IOException("Error writing Riak Object, shutting down bucket load process");
					} else if (isStop(riakObject)) {
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
		}
		
		if (riakObject == null) {
			this.close();
		}
		
		return riakObject;
	}

	private static boolean isStop(IRiakObject riakObject) {
		return riakObject.getBucket().compareTo(STOP_STRING)==0;
	}
	
	private static boolean isError(IRiakObject riakObject) {
		return riakObject.getBucket().compareTo(ERROR_STRING)==0;
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
		private final Iterable<IRiakObject> riakObjects;
		private final ArrayBlockingQueue<IRiakObject> objectQueue;
		private final int stopCount;

		public RiakObjectProducerThread(Iterable<IRiakObject> riakObjects,
					ArrayBlockingQueue<IRiakObject> objectQueue,
					int stopCount) {
			this.riakObjects = riakObjects;
			this.objectQueue = objectQueue;
			this.stopCount = stopCount;
		}
		
		
		public void run() {
			try {
				for (IRiakObject object : this.riakObjects) {
					if (Thread.interrupted()) {
						break;
					}
//					objectQueue.put(object);
					while (!objectQueue.offer(object)) {
						Thread.sleep(10);
					}
				}
				for (int i = 0; i < this.stopCount; ++i) {
					this.objectQueue.put(STOP_OBJECT);
				}
			} catch (InterruptedException e) {
				// no-op
			}
		}
		
	}
	
	private class RiakObjectWriterThread implements Runnable {
		
		private final IClientWriter writer;
		private final ArrayBlockingQueue<IRiakObject> objectQueue;
		private final ArrayBlockingQueue<IRiakObject> returnQueue;
		
		public RiakObjectWriterThread(IClientWriter writer,
					ArrayBlockingQueue<IRiakObject> objectQueue,
					ArrayBlockingQueue<IRiakObject> returnQueue) {
			this.writer = writer;
			this.objectQueue = objectQueue;
			this.returnQueue = returnQueue;
		}

		
		public void run() {
			try {
				try {
					while (!Thread.interrupted()) {
						IRiakObject object = this.objectQueue.poll();
						if (object == null) {
							Thread.sleep(10);
							continue;
						}
						
//						IRiakObject object = this.objectQueue.take();
						if (isStop(object)) {
							break;
						}
						int retries = 0;
						while (!Thread.interrupted() && retries < MAX_RETRIES) {
							try {
								this.writer.storeRiakObject(object);
								break;
							} catch (IOException e) {
								++retries;
								log.error(String.format("Store fail %d on key %s, retrying", retries, object.getKey()), e);
								Thread.sleep(RETRY_WAIT_TIME);
								if (retries > MAX_RETRIES) {
									log.error(String.format("Max retries %d reached", MAX_RETRIES), e);
									throw e;
								}
							} catch (Throwable t) {
								t.printStackTrace();
							}
						}
						while (!this.returnQueue.offer(object)) {
							Thread.sleep(10);
						}
//						this.returnQueue.put(object);
					}
					this.returnQueue.put(STOP_OBJECT);
				} catch (IOException e) {
					this.returnQueue.put(ERROR_OBJECT);
				}
			} catch (InterruptedException e) {
				//no-op
			}
			
		}
		
	}
}
