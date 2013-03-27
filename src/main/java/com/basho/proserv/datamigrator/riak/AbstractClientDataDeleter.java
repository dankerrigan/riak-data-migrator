package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.proserv.datamigrator.io.Key;

public abstract class AbstractClientDataDeleter {
	protected final static int MAX_RETRIES = 3;
	protected final static int RETRY_WAIT_TIME = 10;
	protected final Connection connection;
	protected final Iterable<Key> keySource;
	
	AbstractClientDataDeleter(Connection connection, Iterable<Key> keySource) {
		this.connection = connection;
		this.keySource = keySource;
	}
	
	public abstract Key deleteObject() throws IOException;
}
