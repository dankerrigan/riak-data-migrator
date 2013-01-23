package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.pbc.RiakObject;

public class DummyClientWriter implements IClientWriter {

	@Override
	public void storeRiakObject(RiakObject riakObject) throws IOException {
		//no-op
		
	}

}
