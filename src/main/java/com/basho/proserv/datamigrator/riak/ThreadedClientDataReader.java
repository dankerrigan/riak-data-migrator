package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.BucketLoader;
import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.util.NamedThreadFactory;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedClientDataReader extends AbstractClientDataReader {
	private final Logger log = LoggerFactory.getLogger(ThreadedClientDataReader.class);
	private static final int MAX_QUEUE_SIZE = 10000;
	private static final int WORKER_PROC_MULTIPLER = 2;

	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	private final int workerCount;
	private final LinkedBlockingQueue<Key> keyQueue = 
			new LinkedBlockingQueue<Key>(MAX_QUEUE_SIZE);
	private final LinkedBlockingQueue<IRiakObject> returnQueue = 
			new LinkedBlockingQueue<IRiakObject>(MAX_QUEUE_SIZE);
	
	private static String ERROR_STRING = "ERRORERRORERROR";
	private static ByteString ERROR_FLAG = ByteString.copyFromUtf8(ERROR_STRING);
	private static Key ERROR_KEY = new Key(ERROR_STRING, ERROR_STRING);
	private static IRiakObject ERROR_OBJECT = ConversionUtilWrapper.convertConcreteToInterface(
			new RiakObject(ERROR_FLAG, ERROR_FLAG, ERROR_FLAG, ERROR_FLAG));
	private static String STOP_STRING = "STOPSTOPSTOPSTOPSTOP";
	private static ByteString STOP_FLAG = ByteString.copyFromUtf8(STOP_STRING);
	private static Key STOP_KEY = new Key(STOP_STRING, STOP_STRING);
	private static IRiakObject STOP_OBJECT = ConversionUtilWrapper.convertConcreteToInterface(
			new RiakObject(STOP_FLAG, STOP_FLAG, STOP_FLAG, STOP_FLAG));
	
	private List<Future<Runnable>> threads = new ArrayList<Future<Runnable>>();
	private int stopCount = 0;
	
	public ThreadedClientDataReader(Connection connection, 
								   IClientReaderFactory clientReaderFactory,
								   Iterable<Key> keySource) {
		this(connection, 
			 clientReaderFactory, 
			 keySource, 
			 Runtime.getRuntime().availableProcessors() * WORKER_PROC_MULTIPLER);
	}
	
	public ThreadedClientDataReader(Connection connection, 
				IClientReaderFactory clientReaderFactory, 
				Iterable<Key> keySource,
				int workerCount) {
		super(connection, clientReaderFactory, keySource);
		this.workerCount = workerCount;
		
		this.run();
	}
	
	public IRiakObject readObject() throws IOException {
		IRiakObject riakObject = null;
		
		try {
			riakObject = this.returnQueue.take();
			//fast exit if not flag
			if (isStop(riakObject) || isError(riakObject)) {
				while (!Thread.currentThread().isInterrupted()) {
	//				while ((riakObject = this.returnQueue.poll()) == null) {
	//					Thread.sleep(10);
	//				}
					
					if (isStop(riakObject)) {
						++stopCount;
						if (this.stopCount == this.workerCount) {
							riakObject = null;
							break;
						}
					} else if (isError(riakObject)) {
						this.interruptWorkers();
						throw new IOException();
					} else { 
						break;
					}
					riakObject = this.returnQueue.take();
				}
			}
		} catch (InterruptedException e) {
			//no-op, allow to return 
		} catch (Throwable t) {
			t.printStackTrace();
		}
		finally {
			this.close();
		}
		
		return riakObject;
	}
	
	private static boolean isStop(IRiakObject riakObject) {
		return riakObject.getBucket().compareTo(STOP_STRING) == 0;
	}
	
	private static boolean isError(IRiakObject riakObject) {
		return riakObject.getBucket().compareTo(ERROR_STRING) == 0;
	}
	
	@SuppressWarnings("unchecked")
	private void run() {
		for (int i = 0; i < workerCount; ++i) {
			threadFactory.setNextThreadName(String.format("ClientReaderThread-%d", i));
			this.threads.add((Future<Runnable>) this.executor.submit(new ClientReaderThread(
										this.clientReaderFactory.createClientReader(this.connection),
										this.keyQueue, 
										this.returnQueue)));
		}
		threadFactory.setNextThreadName("KeyReaderThread");
		this.threads.add((Future<Runnable>) this.executor.submit(new KeyReaderThread(this.keySource, this.keyQueue, 
				this.workerCount)));
	}
	
	private void interruptWorkers() {
		for (Future<Runnable> worker : threads) {
			worker.cancel(true);
		}
	}
	private void close() {
		this.executor.shutdown();
	}
		
	private class KeyReaderThread implements Runnable {

		private final Iterable<Key> keySource;
		private final LinkedBlockingQueue<Key> keyQueue;
		private final int stopCount;
		
		public KeyReaderThread(Iterable<Key> keys, LinkedBlockingQueue<Key> keyQueue,
				int stopCount) {
			this.keySource = keys;
			this.keyQueue = keyQueue;
			this.stopCount = stopCount;
		}
		
		@Override
		public void run() {
			try {
				for (Key key : keySource) {
					if (Thread.currentThread().isInterrupted()) {
						break;
					}
						
					while (!keyQueue.offer(key)) {
						Thread.sleep(10);
					}
				}
				for (int i = 0; i < this.stopCount; ++i) {
					this.keyQueue.put(STOP_KEY);
				}
			} catch (InterruptedException e) {
				// no-op, allow to exit
			} catch (Throwable e) { // key source error
				log.error("Error listing keys", e);
				try {
					keyQueue.put(ERROR_KEY);
				} catch (InterruptedException intE) {
					// no-op
				}
			}
		}
	}
	
	private class ClientReaderThread implements Runnable {

		private final IClientReader reader;
		private final LinkedBlockingQueue<Key> keyQueue;
		private final LinkedBlockingQueue<IRiakObject> returnQueue;
		
		public ClientReaderThread(IClientReader reader,
				LinkedBlockingQueue<Key> keyQueue,
				LinkedBlockingQueue<IRiakObject> returnQueue) {
			this.reader = reader;
			this.keyQueue = keyQueue;
			this.returnQueue = returnQueue;
		}
		
		@Override
		public void run() {
			try {
				try {
					while (!Thread.currentThread().isInterrupted()) {
						 Key key = keyQueue.poll();
						 if (key == null) {
							 Thread.sleep(10);
							 continue;
						 }
						 if (key.bucket() == STOP_STRING) {
							 break;
						 } else if (key.bucket() == ERROR_STRING) {
							 returnQueue.put(ERROR_OBJECT);
						 }
						 int retries = 0;
						 while (!Thread.currentThread().isInterrupted() && retries < MAX_RETRIES) {
							 try {
								 RiakObject[] objects = this.reader.fetchRiakObject(key.bucket(), key.key());
								 for (RiakObject object : objects) {
									 IRiakObject riakObject = ConversionUtilWrapper.convertConcreteToInterface(object);
//									 while (!Thread.interrupted() && !returnQueue.offer(riakObject)) {
//										 Thread.sleep(10);
//										 log.info("waiting to put on return queue");
//									 }
									
									returnQueue.put(riakObject);
								 }
								 break;
							 } catch (IOException e) {
								 log.error("Fetch failed, retrying");
								 ++retries;
								 if (retries > MAX_RETRIES) {
									 log.error("Max retries reached");
									 throw e;
								 }
							 }
						 }
					}
					returnQueue.put(STOP_OBJECT);
				} catch (IOException e ) {
					returnQueue.put(ERROR_OBJECT);
				}
			} catch (InterruptedException e) {
				// no-op, allow to exit
			}
			
		}
		
		
	}

}
