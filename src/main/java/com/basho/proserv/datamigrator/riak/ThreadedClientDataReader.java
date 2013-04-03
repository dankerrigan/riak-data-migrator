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
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedClientDataReader extends AbstractClientDataReader {
	private final Logger log = LoggerFactory.getLogger(ThreadedClientDataReader.class);

	private final NamedThreadFactory threadFactory = new NamedThreadFactory();
	private final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	private final int workerCount;
	private final ArrayBlockingQueue<Key> keyQueue;
	private final ArrayBlockingQueue<IRiakObject> returnQueue; 
			
	
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
				Iterable<Key> keySource,
				int workerCount,
				int queueSize) {
		super(connection, clientReaderFactory, keySource);
		this.workerCount = workerCount;
		this.keyQueue = new ArrayBlockingQueue<Key>(queueSize);
		this.returnQueue = new ArrayBlockingQueue<IRiakObject>(queueSize);
		
		this.run();
	}
	
	public IRiakObject readObject() throws IOException {
		IRiakObject riakObject = null;
		
		try {
			riakObject = this.returnQueue.take();
//			while ((riakObject = this.returnQueue.poll()) == null) {
////				log.debug("waiting on return");
//				Thread.sleep(5);
//			}
						
			//fast exit if not flag
			if (isStop(riakObject) || isError(riakObject)) {
				while (!Thread.currentThread().isInterrupted()) {
					if (isStop(riakObject)) {
						++stopCount;
						if (this.stopCount == this.workerCount) {
							this.close();

							riakObject = null;
							break;
						}
					} else if (isError(riakObject)) {
						this.interruptWorkers();
						throw new IOException("Error reading Riak Object, shutting down bucket load process");
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
	
	private class ClientReaderThread implements Runnable {

		private final IClientReader reader;
		private final ArrayBlockingQueue<Key> keyQueue;
		private final ArrayBlockingQueue<IRiakObject> returnQueue;
		
		public ClientReaderThread(IClientReader reader,
				ArrayBlockingQueue<Key> keyQueue,
				ArrayBlockingQueue<IRiakObject> returnQueue) {
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
							 log.debug("Waiting for key");
							 continue;
						 }
						 if (key.bucket() == STOP_STRING) {
							 break;
						 } else if (key.bucket() == ERROR_STRING) {
							 returnQueue.put(ERROR_OBJECT);
						 }
						 
						 IRiakObject[] objects = this.reader.fetchRiakObject(key.bucket(), key.key());
//						 
						 if (objects.length == 1) {
							 returnQueue.put(objects[0]);
						 } else {
							 for (int i = 0; i < objects.length; ++i) {
								 returnQueue.put(objects[i]);
							 }
						 }
					}
					returnQueue.put(STOP_OBJECT);
				} catch (IOException e ) {
					returnQueue.put(ERROR_OBJECT);
				} catch (RiakNotFoundException e) {
					returnQueue.put(ERROR_OBJECT);
				}
			} catch (InterruptedException e) {
				// no-op, allow to exit
			}
			
		}
		
		
	}

}
