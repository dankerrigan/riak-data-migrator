package com.basho.proserv.datamigrator.io;

import java.io.File;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.events.RiakObjectEvent;

public class BatchingRiakObjectReader extends RiakObjectReader implements
		IRiakObjectReader {
	@SuppressWarnings("unused")
	private final Logger log = LoggerFactory.getLogger(BatchingRiakObjectReader.class);
	private static final int DEFAULT_BATCH_SIZE = 10000;
	private final int batchSize;
	
	private Queue<RiakObjectEvent> queue = new LinkedBlockingQueue<RiakObjectEvent>();
	private boolean finished = false;
	
	public BatchingRiakObjectReader(File file, boolean resetVClock) {
		this(file, DEFAULT_BATCH_SIZE, resetVClock);
	}
	
	public BatchingRiakObjectReader(File file, int batchSize, boolean resetVClock) {
		super(file, resetVClock);
		
		this.batchSize = batchSize;
	}

	@Override
	public RiakObjectEvent readRiakObject() {
		if (queue.isEmpty()) {
			fill();
		}
		RiakObjectEvent riakObject = queue.poll();
		if (riakObject == null && finished) {
			return null;
		} else {
			return riakObject;
		}
	}
	
	@Override
	public void close() {
		
	}
	
	private void fill() {
		for (int i = 0; i < this.batchSize; ++i) {
			RiakObjectEvent object = super.readRiakObject();
			if (object != null) {
				this.queue.add(object);
			} else {
				this.finished = true;
				break;
			}
		}
	}

}
