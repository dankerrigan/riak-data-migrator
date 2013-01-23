package com.basho.proserv.datamigrator.io;

import com.basho.riak.pbc.RiakObject;

public interface IRiakObjectReader {
	public RiakObject readRiakObject();
	public void close();
}
