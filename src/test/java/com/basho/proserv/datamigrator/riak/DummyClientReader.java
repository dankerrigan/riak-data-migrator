package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class DummyClientReader implements IClientReader {

	@Override
	public RiakObject[] fetchRiakObject(String bucket, String key)
			throws IOException {
		ByteString vclock = ByteString.copyFromUtf8("ReplaceWithGeneratedVClock");
		RiakObject[] returnVal = new RiakObject[1];
		returnVal[0] = new RiakObject(vclock,
				   ByteString.copyFromUtf8(bucket),
				   ByteString.copyFromUtf8(key),
				   ByteString.copyFromUtf8(""));
		return returnVal;
	}

}
