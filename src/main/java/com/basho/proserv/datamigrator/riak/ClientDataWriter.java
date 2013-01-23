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
	public boolean writeObject() throws IOException {
		this.clientWriter.storeRiakObject(this.objectIterator.next());
		return true;
	}

	@Override
	public void close() throws IOException {
		//no-op
		
	}

}
