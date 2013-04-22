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
import com.basho.proserv.datamigrator.events.RiakObjectEvent;
import com.basho.proserv.datamigrator.util.NamedThreadFactory;

public class ThreadedClientDataWriter extends AbstractClientDataWriter {
	@SuppressWarnings("unused")
	private final Logger log = LoggerFactory.getLogger(ThreadedClientDataWriter.class);

	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	private final int workerCount;
	private final ArrayBlockingQueue<Event> objectQueue;
	private final ArrayBlockingQueue<Event> returnQueue;
	
	private final List<Future<Runnable>> threads = new ArrayList<Future<Runnable>>();
	
	private int stoppedCount = 0;
	
	public ThreadedClientDataWriter(Connection connection,
			IClientWriterFactory clientWriterFactory, 
			Iterable<Event> objectSource, 
			int workerCount,
			int queueSize) {
		super(connection, clientWriterFactory, objectSource);
		this.workerCount = workerCount;
		this.objectQueue = new ArrayBlockingQueue<Event>(queueSize);
		this.returnQueue = new ArrayBlockingQueue<Event>(queueSize);
		
		this.run();
	}
	
	@Override
	public Event writeObject() {
		Event event = Event.NULL;
		try {
			while ((event = this.returnQueue.poll()) == null) {
				Thread.sleep(RETRY_WAIT_TIME);
			}
//			event = this.returnQueue.take();
			
			// Fast exit if not flag
			if (event.isIoErrorEvent() || event.isNullEvent()) {
				while (!Thread.interrupted()) {
					if (event.isIoErrorEvent()) {
						// Abnormal stop, interrupt non-dying workers, close executor
						this.terminate();
						break;
					} else if (event.isNullEvent()) {
						++this.stoppedCount;
						if (this.stoppedCount == this.workerCount) {
							this.close();
							break;
						}
					} else { // else event is a normal event
						break;
					}
					event = this.returnQueue.take();
				}
			}
		} catch (InterruptedException e) {
			//success = false;
		} catch (Throwable e) {
			// not sure what went wrong, print a stack trace
			e.printStackTrace();
			this.close();
		}
				
		return event;
	}
	
	private void interruptWorkers() {
		for (Future<Runnable> future : this.threads) {
			future.cancel(true);
		}
	}
	
	private void close() {
		this.executor.shutdown();
	}

	public void terminate() {
		this.interruptWorkers();
		this.close();
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
		private final Iterable<Event> riakObjects;
		private final ArrayBlockingQueue<Event> objectQueue;
		private final int stopCount;

		public RiakObjectProducerThread(Iterable<Event> riakObjects,
					ArrayBlockingQueue<Event> objectQueue,
					int stopCount) {
			this.riakObjects = riakObjects;
			this.objectQueue = objectQueue;
			this.stopCount = stopCount;
		}
		
		@Override
		public void run() {
			try {
				for (Event object : this.riakObjects) {
					if (Thread.interrupted()) {
						return;
					}
//					objectQueue.put(object);
					while (!objectQueue.offer(object)) {
						Thread.sleep(10);
					}
				}
				for (int i = 0; i < this.stopCount; ++i) {
					this.objectQueue.put(RiakObjectEvent.STOP);
				}
			} catch (InterruptedException e) {
				// no-op
			}
		}
		
	}
	
	private class RiakObjectWriterThread implements Runnable {
		
		private final IClientWriter writer;
		private final ArrayBlockingQueue<Event> objectQueue;
		private final ArrayBlockingQueue<Event> returnQueue;
		
		public RiakObjectWriterThread(IClientWriter writer,
					ArrayBlockingQueue<Event> objectQueue,
					ArrayBlockingQueue<Event> returnQueue) {
			this.writer = writer;
			this.objectQueue = objectQueue;
			this.returnQueue = returnQueue;
		}

		@Override
		public void run() {
			try {
				while (!Thread.interrupted()) {
//					Event object = this.objectQueue.poll();
//					if (object == null) {
//						Thread.sleep(10);
//						continue; 
//					}
					Event object = this.objectQueue.take();
					
					// In the case where events are sourced from a ClientDataReader
					// we can expect IoErrorEvents and ValueErrorEvents, 
					// just pass them back for handling.
					if (!object.isRiakObjectEvent()) {
						this.returnQueue.put(object);
						break;
					}
					
					RiakObjectEvent riakObject = object.asRiakObjectEvent();
					
					if (riakObject.isStopEvent()) {
						break;
					}
											
					Event event = this.writer.storeRiakObject(riakObject);
					
					while (!this.returnQueue.offer(event)) {
						Thread.sleep(10);
					}
//						this.returnQueue.put(object);
				}
				this.returnQueue.put(Event.NULL);
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
		
	}
}
