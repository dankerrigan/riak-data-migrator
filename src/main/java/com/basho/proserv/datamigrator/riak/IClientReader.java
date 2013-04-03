package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.client.IRiakObject;

public interface IClientReader {
	public IRiakObject[] fetchRiakObject(String bucket, String key) 
			throws IOException, RiakNotFoundException, InterruptedException;
}
