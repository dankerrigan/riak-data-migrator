package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.io.Key;

public class ClientDataDeleter extends AbstractClientDataDeleter {
	private final Logger log = LoggerFactory.getLogger(ClientDataDeleter.class);
	private final Iterator<Key> keyIterator;
	
	public ClientDataDeleter(Connection connection, Iterable<Key> keySource) {
		super(connection, keySource);
		this.keyIterator = keySource.iterator();
	}
	
	public Key deleteObject() throws IOException{
		int retryCount = 0;
		Key key = this.keyIterator.next();
		if (key != null) {
			while (retryCount < MAX_RETRIES && !Thread.interrupted())  {
				try {
					this.connection.riakClient.delete(key.bucket(), key.key());
					break;
				} catch (IOException e) {
					++retryCount;
					if (retryCount > MAX_RETRIES) {
						log.error(String.format("Max retries %d reached.", e));
						throw e;
					}
					log.error(String.format("Store fail %d on key %s, retrying", retryCount, key.key()), e);
				}
			}
		}
		
		return key;
	}
}
