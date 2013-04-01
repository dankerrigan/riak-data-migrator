package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.proserv.datamigrator.io.Key;

public class ClientDeleter implements IClientDeleter {
	private final Connection connection;
	
	public ClientDeleter(Connection connection) {
		this.connection = connection;
	}
	
	public Key deleteKey(Key key) throws IOException {
		this.connection.riakClient.delete(key.bucket(), key.key());
		return key;
	}

}
