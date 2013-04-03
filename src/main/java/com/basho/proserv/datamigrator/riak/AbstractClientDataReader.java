package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.riak.client.IRiakObject;

public abstract class AbstractClientDataReader {
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
	public abstract IRiakObject readObject() throws IOException, RiakNotFoundException, InterruptedException;
//	public abstract void close() throws IOException;
}
