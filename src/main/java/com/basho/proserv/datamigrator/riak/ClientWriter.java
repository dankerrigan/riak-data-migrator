package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.pbc.RiakObject;

import static com.basho.riak.client.raw.pbc.ConversionUtilWrapper.convertConcreteToInterface;

public class ClientWriter implements IClientWriter {

	private final Connection connection;
	
	public ClientWriter(Connection connection) {
		this.connection = connection;
	}
	
	@Override
	public void storeRiakObject(RiakObject riakObject) throws IOException {
		this.connection.riakClient.store(convertConcreteToInterface(riakObject));
	}

}
