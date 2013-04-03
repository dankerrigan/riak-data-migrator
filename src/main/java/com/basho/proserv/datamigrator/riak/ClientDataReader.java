package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.riak.client.IRiakObject;

public class ClientDataReader extends AbstractClientDataReader {
	private final Logger log = LoggerFactory.getLogger(ClientDataReader.class);
	
	private final Queue<IRiakObject> queuedObjects = new LinkedBlockingQueue<IRiakObject>();
	private final IClientReader reader;
	private final Iterator<Key> keyIterator;
	
	public ClientDataReader(Connection connection,
			IClientReaderFactory clientReaderFactory,
			Iterable<Key> keySource) {
		super(connection, clientReaderFactory, keySource);
		
		this.reader = clientReaderFactory.createClientReader(connection);
		this.keyIterator = keySource.iterator();
	}

	
	@Override
	public IRiakObject readObject() throws IOException, RiakNotFoundException, InterruptedException {
		if (queuedObjects.isEmpty()) {
			Key key = this.keyIterator.next();
			
			IRiakObject[] objects = this.reader.fetchRiakObject(key.bucket(), key.key());
			for (int i = 0; i < objects.length; ++i) {
				queuedObjects.add(objects[i]);
			}
			
//			for (RiakObject obj : objects) {
//				IRiakObject riakObject = ConversionUtilWrapper.convertConcreteToInterface(obj);
//				queuedObjects.add(riakObject);
//			}
		}
		return queuedObjects.remove();
	}
	
}
