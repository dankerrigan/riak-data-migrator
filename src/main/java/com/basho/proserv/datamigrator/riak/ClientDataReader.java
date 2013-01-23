package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import com.basho.riak.pbc.RiakObject;

public class ClientDataReader extends AbstractClientDataReader {

	private final Queue<RiakObject> queuedObjects = new LinkedBlockingQueue<RiakObject>();
	private final IClientReader reader;
	private final Iterator<String> keyIterator;
	
	public ClientDataReader(Connection connection,
			IClientReaderFactory clientReaderFactory, String bucket,
			Iterable<String> keySource) {
		super(connection, clientReaderFactory, bucket, keySource);
		
		this.reader = clientReaderFactory.createClientReader(connection);
		this.keyIterator = keySource.iterator();
	}

	
	
	@Override
	public RiakObject readObject() throws IOException {
		if (queuedObjects.isEmpty()) {
			try {
				String key = this.keyIterator.next();
				RiakObject[] objects = this.reader.fetchRiakObject(this.bucket, key);
				
				for (RiakObject obj : objects) {
					queuedObjects.add(obj);
				}
				
			} catch (NoSuchElementException e) { // iterator finished
				return null;
			}
		}
		return queuedObjects.remove();
	}



	@Override
	public void close() throws IOException {
		//no-op
	}

	
}
