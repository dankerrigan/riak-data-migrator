package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.pbc.RiakObject;

public interface IClientWriter {
	public RiakObject storeRiakObject(RiakObject riakObject) throws IOException;
}
