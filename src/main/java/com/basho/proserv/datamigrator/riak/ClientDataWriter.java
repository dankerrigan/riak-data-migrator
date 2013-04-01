package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.IRiakObject;

public class ClientDataWriter extends AbstractClientDataWriter {
	private final Logger log = LoggerFactory.getLogger(ClientDataWriter.class);

	private final Iterator<IRiakObject> objectIterator;
	private final IClientWriter clientWriter;
	
	public ClientDataWriter(Connection connection,
			IClientWriterFactory clientWriterFactory,
			Iterable<IRiakObject> objectSource) {
		super(connection, clientWriterFactory, objectSource);
		
		this.objectIterator = this.objectSource.iterator();
		this.clientWriter = clientWriterFactory.createClientWriter(connection);
	}

	@Override
	public IRiakObject writeObject() throws IOException {
		IRiakObject object = this.objectIterator.next();
		int retries = 0;
		while (!Thread.interrupted() && retries < MAX_RETRIES) {
			try {
				this.clientWriter.storeRiakObject(object);
				break;
			} catch (IOException e) {
				++retries;
				if (retries > MAX_RETRIES) {
					log.error("Max retries reached", e);
					throw e;
				}
			}
		}
		return object;
	}


}
