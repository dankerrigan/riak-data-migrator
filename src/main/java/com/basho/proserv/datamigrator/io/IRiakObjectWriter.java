package com.basho.proserv.datamigrator.io;

import com.basho.riak.pbc.RiakObject;

public interface IRiakObjectWriter {
	
	public boolean writeRiakObject(RiakObject riakObject);
	public void close();

}
