package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.BucketDumper;
import com.basho.proserv.datamigrator.io.Key;
import com.basho.riak.pbc.RiakObject;

public class ClientDataReader extends AbstractClientDataReader {
	private final Logger log = LoggerFactory.getLogger(BucketDumper.class);
	
	private final Queue<RiakObject> queuedObjects = new LinkedBlockingQueue<RiakObject>();
	private final IClientReader reader;
	private final Iterator<Key> keyIterator;
	
	public ClientDataReader(Connection connection,
			IClientReaderFactory clientReaderFactory,
			Iterable<Key> keySource) {
		super(connection, clientReaderFactory, keySource);
		
		this.reader = clientReaderFactory.createClientReader(connection);
		this.keyIterator = keySource.iterator();
	}

	
	@Override
	public RiakObject readObject() throws IOException {
		if (queuedObjects.isEmpty()) {
			int retries = 0;
			while (retries < MAX_RETRIES) {
				try {
					Key key = this.keyIterator.next();
					RiakObject[] objects = this.reader.fetchRiakObject(key.bucket(), key.key());
					
					for (RiakObject obj : objects) {
						queuedObjects.add(obj);
					}
					break;
				} catch (NoSuchElementException e) { // iterator finished
					return null;
				} catch (IOException e) {
					++retries;
					if (retries > MAX_RETRIES) {
						log.error("Max retries reached");
						throw e;
					}
				}
			}
		}
		return queuedObjects.remove();
	}
	
}
