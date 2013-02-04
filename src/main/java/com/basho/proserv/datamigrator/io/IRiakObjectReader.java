package com.basho.proserv.datamigrator.io;

import com.basho.riak.client.IRiakObject;

public interface IRiakObjectReader {
	public IRiakObject readRiakObject();
	public void close();
}
