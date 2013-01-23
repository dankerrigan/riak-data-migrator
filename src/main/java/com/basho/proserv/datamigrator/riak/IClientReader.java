package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.pbc.RiakObject;

public interface IClientReader {
	public RiakObject[] fetchRiakObject(String bucket, String key) 
			throws IOException;
}
