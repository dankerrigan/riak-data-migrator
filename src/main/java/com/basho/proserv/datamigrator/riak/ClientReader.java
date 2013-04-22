package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.Configuration;
import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.events.IoErrorEvent;
import com.basho.proserv.datamigrator.events.RiakObjectEvent;
import com.basho.proserv.datamigrator.events.ValueErrorEvent;
import com.basho.proserv.datamigrator.io.Key;
import com.basho.riak.client.raw.RiakResponse;

public class ClientReader implements IClientReader {
	private final Logger log = LoggerFactory.getLogger(ClientReader.class);
	private final Connection connection;
	
	public ClientReader(Connection connection) {
		this.connection = connection;
	}

	@Override
	public Event fetchRiakObject(String bucket, String key) 
			throws InterruptedException {
		return this.fetchRiakObject(bucket, key, 0);
	}
	
	public Event fetchRiakObject(String bucket, String key, int retryCount) 
			throws InterruptedException {
		try {
			RiakResponse resp = this.connection.riakClient.fetch(bucket, key);
			if (resp.hasValue()) {
				return new RiakObjectEvent(resp.getRiakObjects());
			} else {
				throw new RiakNotFoundException(bucket, key);
			}
		} catch (IOException e) {
			if (retryCount < Configuration.MAX_RETRY) {
				log.warn(String.format("Fetch fail %d, Riak Exception, on key %s / %s , retrying", retryCount, bucket, key), e);
				Thread.sleep(Configuration.RETRY_WAIT_MILLIS);
				return fetchRiakObject(bucket, key, retryCount + 1);
			} else {
				log.error(String.format("Max retries %d reached on key %s / %s", Configuration.MAX_RETRY, bucket, key, e));
				return new IoErrorEvent(new Key(bucket, key), e);
			}
		} catch (RiakNotFoundException e) {
			if (retryCount < Configuration.MAX_RETRY) {
				log.warn(String.format("Fetch fail %d, Not Found, on key %s / %s, retrying", retryCount, bucket, key), e);
				Thread.sleep(Configuration.RETRY_WAIT_MILLIS);
				return fetchRiakObject(bucket, key, retryCount + 1);
			} else {
				log.error(String.format("Max retries %d reached on key %s / %s", Configuration.MAX_RETRY, bucket, key, e));
				return new ValueErrorEvent(new Key(bucket, key));
			}
		}
	}

}
