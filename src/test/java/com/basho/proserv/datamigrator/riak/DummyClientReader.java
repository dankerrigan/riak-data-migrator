package com.basho.proserv.datamigrator.riak;

import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.events.RiakObjectEvent;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.google.protobuf.ByteString;

public class DummyClientReader implements IClientReader {

	@Override
	public Event fetchRiakObject(String bucket, String key) {
		ByteString vclock = ByteString.copyFromUtf8("ReplaceWithGeneratedVClock");
		IRiakObject[] returnVal = new IRiakObject[1];
		returnVal[0] = RiakObjectBuilder.newBuilder(bucket, key)
			.withVClock(vclock.toByteArray())
			.withValue("")
			.build();
		
		return new RiakObjectEvent(returnVal);
	}

}
