package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.pbc.RiakObject;

public abstract class AbstractClientDataReader {
	
	protected final Connection connection;
	protected final IClientReaderFactory clientReaderFactory;
	protected final Iterable<String> keySource;
	protected final String bucket;
	
	AbstractClientDataReader(Connection connection, 
							IClientReaderFactory clientReaderFactory,
							String bucket, 
							Iterable<String> keySource) {
		this.connection = connection;
		this.clientReaderFactory = clientReaderFactory;
		this.keySource = keySource;
		this.bucket = bucket;
	}
	public abstract RiakObject readObject() throws IOException;
	public abstract void close() throws IOException;
}
