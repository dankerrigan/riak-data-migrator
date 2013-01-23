package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedClientDataReader extends AbstractClientDataReader {
	private static final int MAX_QUEUE_SIZE = 10000;
	private static final int WORKER_PROC_MULTIPLER = 2;
	
	private final ExecutorService executor = Executors.newCachedThreadPool();
	private final int workerCount;
	private final LinkedBlockingQueue<String> keyQueue = 
			new LinkedBlockingQueue<String>(MAX_QUEUE_SIZE);
	private final LinkedBlockingQueue<RiakObject> returnQueue = 
			new LinkedBlockingQueue<RiakObject>(MAX_QUEUE_SIZE);
	
	private static ByteString ERROR_FLAG = ByteString.copyFromUtf8("ERRORERRORERROR");
	private static String STOP_STRING = "STOPSTOPSTOPSTOPSTOP";
	private static ByteString STOP_FLAG = ByteString.copyFromUtf8(STOP_STRING);
	
	private int stopCount = 0;
	
	public ThreadedClientDataReader(Connection connection, 
								   IClientReaderFactory clientReaderFactory,
								   String bucket, 
								   Iterable<String> keySource) {
		this(connection, 
			 clientReaderFactory, 
			 bucket, 
			 keySource, 
			 Runtime.getRuntime().availableProcessors() * WORKER_PROC_MULTIPLER);
	}
	
	public ThreadedClientDataReader(Connection connection, 
				IClientReaderFactory clientReaderFactory, 
				String bucket, Iterable<String> keySource,
				int workerCount) {
		super(connection, clientReaderFactory, bucket, keySource);
		this.workerCount = workerCount;
		
		this.run();
	}
	
	public RiakObject readObject() throws IOException {
		RiakObject riakObject = null;
		
		try {
			while (!Thread.currentThread().isInterrupted()) {
//				while ((riakObject = this.returnQueue.poll()) == null) {
//					Thread.sleep(10);
//				}
				riakObject = this.returnQueue.take();
				if (riakObject.getBucketBS() == STOP_FLAG) {
					++stopCount;
					if (this.stopCount == this.workerCount) {
						riakObject = null;
						break;
					}
				} else if (riakObject.getBucketBS() == ERROR_FLAG) {
					throw new IOException();
				} else { 
					break;
				}
			}
		} catch (InterruptedException e) {
			//no-op, allow to return 
		}
		
		return riakObject;
	}
	
	private void run() {
		for (int i = 0; i < workerCount; ++i) {
			this.executor.execute(new ClientReaderThread(
										this.clientReaderFactory.createClientReader(this.connection), 
										bucket, 
										this.keyQueue, 
										this.returnQueue));
		}
		this.executor.execute(new KeyReaderThread(this.keySource, this.keyQueue, 
				this.workerCount));
	}
	
	public  void close() {
		this.executor.shutdown();
	}
	
	private class KeyReaderThread implements Runnable {

		private final Iterable<String> keySource;
		private final LinkedBlockingQueue<String> keyQueue;
		private final int stopCount;
		
		public KeyReaderThread(Iterable<String> keys, LinkedBlockingQueue<String> keyQueue,
				int stopCount) {
			this.keySource = keys;
			this.keyQueue = keyQueue;
			this.stopCount = stopCount;
		}
		
		@Override
		public void run() {
			try {
				for (String key : keySource) {
					if (Thread.currentThread().isInterrupted()) {
						break;
					}
						
					while (!keyQueue.offer(key)) {
						Thread.sleep(10);
					}
				}
				for (int i = 0; i < this.stopCount; ++i) {
					this.keyQueue.put(STOP_STRING);
				}
			} catch (InterruptedException e) {
				// no-op, allow to exit
			}
		}
	}
	
	private class ClientReaderThread implements Runnable {

		private final IClientReader reader;
		private final String bucket;
		private final LinkedBlockingQueue<String> keyQueue;
		private final LinkedBlockingQueue<RiakObject> returnQueue;
		
		public ClientReaderThread(IClientReader reader,
				String bucket,
				LinkedBlockingQueue<String> keyQueue,
				LinkedBlockingQueue<RiakObject> returnQueue) {
			this.reader = reader;
			this.bucket = bucket;
			this.keyQueue = keyQueue;
			this.returnQueue = returnQueue;
		}
		
		@Override
		public void run() {
			try {
				try {
					while (!Thread.currentThread().isInterrupted()) {
						 String key = keyQueue.take();
						 if (key == STOP_STRING) {
							 break;
						 }
						 RiakObject[] objects = this.reader.fetchRiakObject(bucket, key);
						 for (RiakObject riakObject : objects) {
							 returnQueue.put(riakObject);
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
