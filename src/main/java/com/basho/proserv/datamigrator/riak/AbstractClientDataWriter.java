package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.pbc.RiakObject;

public abstract class AbstractClientDataWriter {
	protected final Connection connection;
	protected final IClientWriterFactory clientWriterFactory;
	protected final Iterable<RiakObject> objectSource;
	
	AbstractClientDataWriter(Connection connection, 
							IClientWriterFactory clientWriterFactory,
							Iterable<RiakObject> objectSource) {
		this.connection = connection;
		this.clientWriterFactory = clientWriterFactory;
		this.objectSource = objectSource;
	}
	public abstract boolean writeObject() throws IOException;
	public abstract void close() throws IOException;
	
	
}
