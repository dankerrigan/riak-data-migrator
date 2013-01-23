package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.pbc.RiakObject;

import static com.basho.riak.client.raw.pbc.ConversionUtilWrapper.convertInterfaceToConcrete;

public class ClientReader implements IClientReader {
	
	private final Connection connection;
	
	public ClientReader(Connection connection) {
		this.connection = connection;
	}

	@Override
	public RiakObject[] fetchRiakObject(String bucket, String key) 
			throws IOException {
		RiakResponse resp = this.connection.riakClient.fetch(bucket, key);
		IRiakObject[] inObjects = resp.getRiakObjects();
		RiakObject[] outObjects = new RiakObject[inObjects.length];
		for (int i = 0; i < inObjects.length; ++i) {
			outObjects[i] = convertInterfaceToConcrete(inObjects[i]);
		}
		return outObjects;
	}

}
