package com.basho.proserv.datamigrator.io;

import com.basho.riak.client.IRiakObject;

public interface IRiakObjectWriter {
	
	public boolean writeRiakObject(IRiakObject riakObject);
	public void close();

}
