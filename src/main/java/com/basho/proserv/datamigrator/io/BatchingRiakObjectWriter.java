package com.basho.proserv.datamigrator.io;

import java.io.File;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import com.basho.riak.client.IRiakObject;

public class BatchingRiakObjectWriter extends RiakObjectWriter implements IRiakObjectWriter{

	private final static int DEFAULT_QUEUE_SIZE = 10000;
	private final int batchSize;
	
	private final Queue<IRiakObject> queue = new LinkedBlockingQueue<IRiakObject>();
	
	public BatchingRiakObjectWriter(File file) {
		this(file, DEFAULT_QUEUE_SIZE);
	}
	
	public BatchingRiakObjectWriter(File file, int batchSize) {
		super(file);
		this.batchSize = batchSize;
	}
	
	@Override
	public boolean writeRiakObject(IRiakObject riakObject) {
		queue.add(riakObject);
		if (queue.size() >= this.batchSize) {
			flush();
		}
		return true;
	}

	@Override
	public void close() {
		flush();
		super.close();
		
	}
	
	private boolean[] flush() {
		boolean[] returnValues = new boolean[this.queue.size()];
		
		int size = this.queue.size();
		for (int i = 0; i < size; ++i) {
			super.writeRiakObject(queue.poll());
		}
		
		return returnValues;
	}

}
