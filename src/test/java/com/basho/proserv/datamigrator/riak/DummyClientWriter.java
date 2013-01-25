package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class DummyClientWriter implements IClientWriter {

	@Override
	public RiakObject storeRiakObject(RiakObject riakObject) throws IOException {
		return new RiakObject(ByteString.copyFromUtf8(""),
				ByteString.copyFromUtf8(""),
				ByteString.copyFromUtf8(""));
	}

}
