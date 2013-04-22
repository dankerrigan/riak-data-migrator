package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.util.NamedThreadFactory;

public class ThreadedClientDataDeleter extends AbstractClientDataDeleter {
	private final Logger log = LoggerFactory.getLogger(ThreadedClientDataDeleter.class);
	private static final int MAX_QUEUE_SIZE = 1000;
	private static final int WORKER_PROC_MULTIPLER = 2;

	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	private final int workerCount;
	private final ArrayBlockingQueue<Key> keyQueue = 
			new ArrayBlockingQueue<Key>(MAX_QUEUE_SIZE);
	private final ArrayBlockingQueue<Key> returnQueue = 
			new ArrayBlockingQueue<Key>(MAX_QUEUE_SIZE);
	
	private static String ERROR_STRING = "ERRORERRORERROR";
	private static Key ERROR_KEY = new Key(ERROR_STRING, ERROR_STRING);
	private static String STOP_STRING = "STOPSTOPSTOPSTOPSTOP";
	private static Key STOP_KEY = new Key(STOP_STRING, STOP_STRING);
	
	private List<Future<Runnable>> threads = new ArrayList<Future<Runnable>>();
	private int stopCount = 0;
	
	public ThreadedClientDataDeleter(Connection connection, 
								   Iterable<Key> keySource) {
		this(connection,  
			 keySource, 
			 Runtime.getRuntime().availableProcessors() * WORKER_PROC_MULTIPLER);
	}
	
	public ThreadedClientDataDeleter(Connection connection, 
				Iterable<Key> keySource,
				int workerCount) {
		super(connection, keySource);
		this.workerCount = workerCount;
		
		this.run();
	}
	
	public Key deleteObject() throws IOException {
		Key key = null;
		
		try {
//			riakObject = this.returnQueue.take();
			while ((key = this.returnQueue.poll()) == null) {
//				log.debug("waiting on return");
				Thread.sleep(10);
			}
			
			//fast exit if not flag
			if (isStop(key) || isError(key)) {
				while (!Thread.currentThread().isInterrupted()) {
					
					if (isStop(key)) {
						++stopCount;
						if (this.stopCount == this.workerCount) {
							key = null;
							this.close();
							break;
						}
					} else if (isError(key)) {
						this.interruptWorkers();
						this.close();
						throw new IOException("Error deleting Riak Object, shutting down bucket load process");
					} else { 
						break;
					}
					key = this.returnQueue.take();
				}
			}
		} catch (InterruptedException e) {
			//no-op, allow to return 
		} catch (Throwable t) {
			t.printStackTrace();
			this.close();
		}
		
		return key;
	}
	
	private static boolean isStop(Key key) {
		return key.bucket().compareTo(STOP_STRING) == 0;
	}
	
	private static boolean isError(Key key) {
		return key.bucket().compareTo(ERROR_STRING) == 0;
	}
	
	@SuppressWarnings("unchecked")
	private void run() {
		for (int i = 0; i < workerCount; ++i) {
			threadFactory.setNextThreadName(String.format("ClientDeleterThread-%d", i));
			this.threads.add((Future<Runnable>) this.executor.submit(new ClientDeleterThread(
										new ClientDeleter(this.connection),
										this.keyQueue, 
										this.returnQueue)));
		}
		threadFactory.setNextThreadName("KeyDeleterThread");
		this.threads.add((Future<Runnable>) this.executor.submit(
					new KeyReaderThread(this.keySource, this.keyQueue, 
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
		private final ArrayBlockingQueue<Key> keyQueue;
		private final int stopCount;
		
		public KeyReaderThread(Iterable<Key> keys, ArrayBlockingQueue<Key> keyQueue,
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
	
	private class ClientDeleterThread implements Runnable {

		private final IClientDeleter deleter;
		private final ArrayBlockingQueue<Key> keyQueue;
		private final ArrayBlockingQueue<Key> returnQueue;
		
		public ClientDeleterThread(IClientDeleter deleter,
				ArrayBlockingQueue<Key> keyQueue,
				ArrayBlockingQueue<Key> returnQueue) {
			this.deleter = deleter;
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
							 returnQueue.put(ERROR_KEY);
						 }
						 int retries = 0;
						 while (!Thread.currentThread().isInterrupted() && retries < MAX_RETRIES) {
							 try {
								 Key returnKey = this.deleter.deleteKey(key);
								 while (!Thread.interrupted() && !returnQueue.offer(returnKey)) {
									 Thread.sleep(10);
								 }
								 break;
							 } catch (IOException e) {
								 ++retries;
								 log.error(String.format("Delete fail %d on key %s, retrying", retries, key.key()), e);
								 Thread.sleep(RETRY_WAIT_TIME);
								 if (retries > MAX_RETRIES) {
									 log.error(String.format("Max retries %d reached", MAX_RETRIES), e);
									 throw e;
								 }
							 }
						 }
					}
					returnQueue.put(STOP_KEY);
				} catch (IOException e ) {
					returnQueue.put(ERROR_KEY);
				}
			} catch (InterruptedException e) {
				// no-op, allow to exit
			}
			
		}
		
		
	}

}
