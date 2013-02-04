package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.client.IRiakObject;

public abstract class AbstractClientDataWriter {
	protected final static int MAX_RETRIES = 3;
	protected final Connection connection;
	protected final IClientWriterFactory clientWriterFactory;
	protected final Iterable<IRiakObject> objectSource;
	
	AbstractClientDataWriter(Connection connection, 
							IClientWriterFactory clientWriterFactory,
							Iterable<IRiakObject> objectSource) {
		this.connection = connection;
		this.clientWriterFactory = clientWriterFactory;
		this.objectSource = objectSource;
	}
	public abstract IRiakObject writeObject() throws IOException;
//	public abstract void close() throws IOException;
	
	
}
