package com.basho.proserv.datamigrator.riak;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.events.Event;

public class ClientDataWriter extends AbstractClientDataWriter {
	@SuppressWarnings("unused")
	private final Logger log = LoggerFactory.getLogger(ClientDataWriter.class);

	private final Iterator<Event> objectIterator;
	private final IClientWriter clientWriter;
	
	public ClientDataWriter(Connection connection,
			IClientWriterFactory clientWriterFactory,
			Iterable<Event> objectSource) {
		super(connection, clientWriterFactory, objectSource);
		
		this.objectIterator = this.objectSource.iterator();
		this.clientWriter = clientWriterFactory.createClientWriter(connection);
	}

	@Override
	public Event writeObject() {
		try {
			Event event = this.objectIterator.next();
			
			if (!event.isRiakObjectEvent()) {
				return event;
			}
			
			return this.clientWriter.storeRiakObject(event.asRiakObjectEvent());
			
		} catch (NoSuchElementException e) {
			return Event.NULL;
		}
	}


}
