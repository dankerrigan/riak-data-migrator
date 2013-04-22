package com.basho.proserv.datamigrator.riak;

import com.basho.proserv.datamigrator.events.Event;

public abstract class AbstractClientDataWriter {
	protected final static int MAX_RETRIES = 3;
	protected final static int RETRY_WAIT_TIME = 10; 
	protected final Connection connection;
	protected final IClientWriterFactory clientWriterFactory;
	protected final Iterable<Event> objectSource;
	
	protected final long valueErrors = 0;
	protected final long ioErrors = 0;
	
	AbstractClientDataWriter(Connection connection, 
							IClientWriterFactory clientWriterFactory,
							Iterable<Event> objectSource) {
		this.connection = connection;
		this.clientWriterFactory = clientWriterFactory;
		this.objectSource = objectSource;
	}
	
	public abstract Event writeObject();
	
	
	
}
