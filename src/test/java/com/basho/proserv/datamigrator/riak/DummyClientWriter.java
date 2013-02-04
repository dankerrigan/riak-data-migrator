package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class DummyClientWriter implements IClientWriter {

	@Override
	public IRiakObject storeRiakObject(IRiakObject riakObject) throws IOException {
		return riakObject;
	}

}
