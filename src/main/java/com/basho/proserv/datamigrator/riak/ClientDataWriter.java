package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.Iterator;

import com.basho.riak.pbc.RiakObject;

public class ClientDataWriter extends AbstractClientDataWriter {

	private final Iterator<RiakObject> objectIterator;
	private final IClientWriter clientWriter;
	
	public ClientDataWriter(Connection connection,
			IClientWriterFactory clientWriterFactory,
			Iterable<RiakObject> objectSource) {
		super(connection, clientWriterFactory, objectSource);
		
		this.objectIterator = this.objectSource.iterator();
		this.clientWriter = clientWriterFactory.createClientWriter(connection);
	}

	@Override
	public RiakObject writeObject() throws IOException {
		RiakObject object = this.objectIterator.next();
		this.clientWriter.storeRiakObject(object);
		return object;
	}


}
