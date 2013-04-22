package com.basho.proserv.datamigrator.riak;

import java.util.Iterator;

import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.io.Key;

public abstract class AbstractClientDataReader implements Iterable<Event> {
	protected final static int MAX_RETRIES = 3;
	protected final static int RETRY_WAIT_TIME = 10;
	protected final Connection connection;
	protected final IClientReaderFactory clientReaderFactory;
	protected final Iterable<Key> keySource;
	
	AbstractClientDataReader(Connection connection, 
							IClientReaderFactory clientReaderFactory,
							Iterable<Key> keySource) {
		this.connection = connection;
		this.clientReaderFactory = clientReaderFactory;
		this.keySource = keySource;
	}
	
	public abstract Event readObject() throws InterruptedException;
	
	@Override
	public Iterator<Event> iterator() {
		return new EventIterator(this);
	}
	
	private class EventIterator implements Iterator<Event> {

		private final AbstractClientDataReader dataReader;
		private Event nextEvent = Event.NULL;
		
		public EventIterator(AbstractClientDataReader dataReader) {
			this.dataReader = dataReader;
			
			try {
				this.nextEvent = this.dataReader.readObject();
			} catch (InterruptedException e) {
				//no-op, next Event will be default of NULL
			}
		}
		
		@Override
		public boolean hasNext() {
			return !this.nextEvent.isNullEvent();
		}

		@Override
		public Event next() {
			Event currentEvent = this.nextEvent;
			try {
				this.nextEvent = this.dataReader.readObject();
			} catch (InterruptedException e) {
				this.nextEvent = Event.NULL;
			}
			
			return currentEvent; 
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Remove is unimplemented");
		}
		
	}
}
