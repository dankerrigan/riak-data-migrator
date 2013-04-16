package com.basho.proserv.datamigrator.riak;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.io.Key;

public class ClientDataReader extends AbstractClientDataReader {
	private final Logger log = LoggerFactory.getLogger(ClientDataReader.class);
	
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
	public Event readObject() throws InterruptedException {
		Event event = Event.NULL;
		try {
			Key key = this.keyIterator.next();
			
			event = this.reader.fetchRiakObject(key.bucket(), key.key());
		} catch (NoSuchElementException e) {
			//no-op
		}

		return event;
	}
	
}
