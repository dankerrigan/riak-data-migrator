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
	private final LinkedBlockingQueue<RiakObject> returnQueue = 
			new LinkedBlockingQueue<RiakObject>(MAX_QUEUE_SIZE);
	
	private static String ERROR_STRING = "ERRORERRORERROR";
	private static ByteString ERROR_FLAG = ByteString.copyFromUtf8(ERROR_STRING);
	private static Key ERROR_KEY = new Key(ERROR_STRING, ERROR_STRING);
	private static String STOP_STRING = "STOPSTOPSTOPSTOPSTOP";
	private static ByteString STOP_FLAG = ByteString.copyFromUtf8(STOP_STRING);
	private static Key STOP_KEY = new Key(STOP_STRING, STOP_STRING);
	
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
	
	public RiakObject readObject() throws IOException {
		RiakObject riakObject = null;
		
		try {
			riakObject = this.returnQueue.take();
			//fast exit if not flag
			if (riakObject.getBucketBS() == STOP_FLAG ||
					riakObject.getBucketBS() == ERROR_FLAG) {
				while (!Thread.currentThread().isInterrupted()) {
	//				while ((riakObject = this.returnQueue.poll()) == null) {
	//					Thread.sleep(10);
	//				}
					
					if (riakObject.getBucketBS() == STOP_FLAG) {
						++stopCount;
						if (this.stopCount == this.workerCount) {
							riakObject = null;
							break;
						}
					} else if (riakObject.getBucketBS() == ERROR_FLAG) {
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
		} finally {
			this.close();
		}
		
		return riakObject;
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
		private final LinkedBlockingQueue<RiakObject> returnQueue;
		
		public ClientReaderThread(IClientReader reader,
				LinkedBlockingQueue<Key> keyQueue,
				LinkedBlockingQueue<RiakObject> returnQueue) {
			this.reader = reader;
			this.keyQueue = keyQueue;
			this.returnQueue = returnQueue;
		}
		
		@Override
		public void run() {
			try {
				try {
					while (!Thread.currentThread().isInterrupted()) {
						 Key key = keyQueue.take();
						 if (key.bucket() == STOP_STRING) {
							 break;
						 } else if (key.bucket() == ERROR_STRING) {
							 returnQueue.put(new RiakObject(ERROR_FLAG, ERROR_FLAG, ERROR_FLAG));
						 }
						 int retries = 0;
						 while (!Thread.currentThread().isInterrupted() && retries < MAX_RETRIES) {
							 try {
								 RiakObject[] objects = this.reader.fetchRiakObject(key.bucket(), key.key());
								 for (RiakObject riakObject : objects) {
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
					returnQueue.put(new RiakObject(STOP_FLAG, STOP_FLAG, STOP_FLAG));
				} catch (IOException e ) {
					returnQueue.put(new RiakObject(ERROR_FLAG, ERROR_FLAG, ERROR_FLAG));
				}
			} catch (InterruptedException e) {
				// no-op, allow to exit
			}
			
		}
		
		
	}

}
