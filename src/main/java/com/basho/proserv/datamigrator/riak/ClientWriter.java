package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.Configuration;
import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.events.IoErrorEvent;
import com.basho.proserv.datamigrator.events.RiakObjectEvent;
import com.basho.proserv.datamigrator.io.Key;
import com.basho.riak.client.IRiakObject;

public class ClientWriter implements IClientWriter {

	private final Logger log = LoggerFactory.getLogger(ClientWriter.class);
	private final Connection connection;
	
	private String bucketRename = null;
	
	public ClientWriter(Connection connection) {
		this.connection = connection;
	}
	
	@Override 
	public Event storeRiakObject(RiakObjectEvent riakObject) { 
		return this.storeRiakObject(riakObject, 0);
	}
	
	public Event storeRiakObject(RiakObjectEvent riakObject, int retryCount) {
		IRiakObject[] objects = riakObject.riakObjects();
		Key key = riakObject.key();
		try {
			for (int i = 0; i < objects.length; ++i) {
				if (bucketRename == null) {
					this.connection.riakClient.store(objects[i]);	
				} else {
					IRiakObject renamed = BucketRenamer.renameBucket(objects[i], this.bucketRename);
					this.connection.riakClient.store(renamed);
				}
			}
			return riakObject;
		} catch (IOException e) {
			if (retryCount < Configuration.MAX_RETRY) {
				log.warn(String.format("Store fail %d, Riak Exception, on key %s / %s , retrying", 
						retryCount, key.bucket(), key.key()), e);
				return this.storeRiakObject(riakObject, retryCount + 1);
			} else {
				log.error(String.format("Max store retries %d reached on key %s / %s", Configuration.MAX_RETRY, 
						key.bucket(), key.key(), e));
				return new IoErrorEvent(key, e);
			}
		}
	}

	@Override
	public void setBucketRename(String bucketName) {
		this.bucketRename = bucketName;
	}

}
