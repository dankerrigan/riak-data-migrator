package com.basho.proserv.datamigrator.riak;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.util.NamedThreadFactory;

public class ThreadedClientDataReader extends AbstractClientDataReader {
	@SuppressWarnings("unused")
	private final Logger log = LoggerFactory.getLogger(ThreadedClientDataReader.class);

	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	private final int workerCount;
	private final ArrayBlockingQueue<Key> keyQueue;
	private final ArrayBlockingQueue<Event> returnQueue; 
			
	
//	private static String ERROR_STRING = "ERRORERRORERROR";
//	private static Key ERROR_KEY = new Key(ERROR_STRING, ERROR_STRING);
	
	private static String STOP_STRING = "STOPSTOPSTOPSTOPSTOP";
	private static Key STOP_KEY = new Key(STOP_STRING, STOP_STRING);
	
	
	private List<Future<Runnable>> threads = new ArrayList<Future<Runnable>>();
	private int stopCount = 0;
	
	public ThreadedClientDataReader(Connection connection, 
				IClientReaderFactory clientReaderFactory, 
				Iterable<Key> keySource,
				int workerCount,
				int queueSize) {
		super(connection, clientReaderFactory, keySource);
		this.workerCount = workerCount;
		this.keyQueue = new ArrayBlockingQueue<Key>(queueSize);
		this.returnQueue = new ArrayBlockingQueue<Event>(queueSize);
		
		this.run();
	}
	
	public Event readObject() {
		Event event = Event.NULL;
		
		try {
			while ((event = this.returnQueue.poll()) == null) {
				Thread.sleep(RETRY_WAIT_TIME);
			}
//			event = this.returnQueue.take();
						
			//fast exit if not flag
			if (event.isNullEvent() || event.isIoErrorEvent()) {
				while (!Thread.currentThread().isInterrupted()) {
					if (event.isNullEvent()) {
						++stopCount;
						if (this.stopCount == this.workerCount) {
							this.close();

							break;
						}
					} else if (event.isIoErrorEvent()) {
						// Abnormal stop, interrupt non-dying workers, close executor
						this.terminate();
						
						break;
					} else { // else event is a normal event
						break;
					}
					event = this.returnQueue.take();
				}
			}
		} catch (InterruptedException e) {
			//no-op, allow to return 
		} catch (Throwable t) {
			t.printStackTrace();
			this.close();
		}
		
		
		
		return event;
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
	
	public void terminate() {
		this.interruptWorkers();
		this.close();
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
						return;
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
			}
		}
	}
	
	private class ClientReaderThread implements Runnable {

		private final IClientReader reader;
		private final ArrayBlockingQueue<Key> keyQueue;
		private final ArrayBlockingQueue<Event> returnQueue;
		
		public ClientReaderThread(IClientReader reader,
				ArrayBlockingQueue<Key> keyQueue,
				ArrayBlockingQueue<Event> returnQueue) {
			this.reader = reader;
			this.keyQueue = keyQueue;
			this.returnQueue = returnQueue;
		}
		
		@Override
		public void run() {
			try {
				while (!Thread.currentThread().isInterrupted()) {
//					 Key key = keyQueue.poll();
//					 if (key == null) {
//						 Thread.sleep(10);
//						 log.debug("Waiting for key");
//						 continue;
//					 }
					 Key key = keyQueue.take();
					 if (key.bucket() == STOP_STRING) {
						 break;
					 }
					 
					 Event event = this.reader.fetchRiakObject(key.bucket(), key.key());

					 returnQueue.put(event);
				}
				returnQueue.put(Event.NULL); 
			} catch (InterruptedException e) {
				// no-op, allow to exit
			}
			
		}
		
		
	}

}
