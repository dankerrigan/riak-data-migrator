package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.Configuration;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.RiakResponse;

public class ClientReader implements IClientReader {
	private final Logger log = LoggerFactory.getLogger(ClientReader.class);
	private final Connection connection;
	
	public ClientReader(Connection connection) {
		this.connection = connection;
	}

	@Override
	public IRiakObject[] fetchRiakObject(String bucket, String key) 
			throws IOException, RiakNotFoundException, InterruptedException {
		return this.fetchRiakObject(bucket, key, 0);
	}
	
//	@Override
//	public IRiakObject[] fetchRiakObject(String bucket, String key) 
//			throws IOException, RiakNotFoundException, InterruptedException {
//		return this.connection.riakClient.fetch(bucket, key).getRiakObjects();
//	}
	
	public IRiakObject[] fetchRiakObject(String bucket, String key, int retryCount) 
			throws IOException, RiakNotFoundException, InterruptedException {
		try {
			RiakResponse resp = this.connection.riakClient.fetch(bucket, key);
			if (resp.hasValue()) {
				return resp.getRiakObjects();
			} else {
				throw new RiakNotFoundException(bucket, key);
			}
		} catch (IOException e) {
			if (retryCount < Configuration.MAX_RETRY) {
				log.error(String.format("Fetch fail %d, Riak Exception, on key %s / %s , retrying", retryCount, bucket, key), e);
				Thread.sleep(Configuration.RETRY_WAIT_MILLIS);
				return fetchRiakObject(bucket, key, retryCount + 1);
			} else {
				log.error("Max retries reached", e);
				throw e;
			}
		} catch (RiakNotFoundException e) {
			if (retryCount < Configuration.MAX_RETRY) {
				log.error(String.format("Fetch fail %d, Not Found, on key %s / %s, retrying", retryCount, bucket, key), e);
				Thread.sleep(Configuration.RETRY_WAIT_MILLIS);
				return fetchRiakObject(bucket, key, retryCount + 1);
			} else {
				log.error("Max retries reached", e);
				throw e;
			}
		}
	}

}
