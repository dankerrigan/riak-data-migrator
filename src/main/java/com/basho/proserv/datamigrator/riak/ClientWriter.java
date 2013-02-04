package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.client.IRiakObject;

public class ClientWriter implements IClientWriter {

	private final Connection connection;
	
	public ClientWriter(Connection connection) {
		this.connection = connection;
	}
	
	@Override
	public IRiakObject storeRiakObject(IRiakObject riakObject) throws IOException {
		this.connection.riakClient.store(riakObject);
		return riakObject;
	}

}
