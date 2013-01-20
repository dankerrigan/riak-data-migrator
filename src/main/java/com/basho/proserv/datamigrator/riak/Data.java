package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.IRiakObject;

public class Data {
	private Logger log = LoggerFactory.getLogger(Data.class);
	
	private Connection connection = null;
	
	public Data(Connection connection) {
		this.connection = connection;
	}

	IRiakObject[] getValue(String bucketName, String key) {
		try {
			return this.connection.riakClient.fetch(bucketName, key).getRiakObjects();
		} catch (IOException e) {
			log.error("Could not fetch value", e);
		}
		return null;
	}
	
	Iterable<String> getKeys(String bucketName) {
		try {
			return this.connection.riakClient.listKeys(bucketName);
		} catch (IOException e) {
			log.error("Could not retrieve keys", e);
		}
		return null;
	}
	
	Set<String> getBucketNames() {
		try {
			return this.connection.riakClient.listBuckets();
		} catch (IOException e) {
			log.error("Could not retrieve buckets", e);
		};
		return null;
	}
}
