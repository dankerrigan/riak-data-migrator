package com.basho.proserv.datamigrator.riak;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.basho.proserv.datamigrator.Configuration;
import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.events.RiakObjectEvent;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedClientDataWriterTests {

	@Test
	public void test() throws Exception {
		Connection connection = new Connection();
		int TEST_SIZE = 10000;
		IClientWriterFactory factory = new DummyClientWriterFactory();
		Configuration config = new Configuration();
		config.setRiakWorkerCount(8);
		
		ByteString data = ByteString.copyFromUtf8("DATA");
		
		List<Event> dummyObjects = new ArrayList<Event>();
		for (Integer i = 0; i < TEST_SIZE; ++i) {
			dummyObjects.add(new RiakObjectEvent(
					ConversionUtilWrapper.convertConcreteToInterface(new RiakObject(data, data, data, data))));
		}
		
		ThreadedClientDataWriter writer = new ThreadedClientDataWriter(connection,
				factory,
				dummyObjects, config.getRiakWorkerCount(), config.getQueueSize());
	
		int writtenCount = 0;
		
		@SuppressWarnings("unused")
		Event event = Event.NULL;
		while (!(event = writer.writeObject()).isNullEvent()) {
			++writtenCount;
		}
		
		System.out.println("Written Count: "  + writtenCount);
		assertTrue(writtenCount == TEST_SIZE);
	}
	

}
