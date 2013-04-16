package com.basho.proserv.datamigrator.riak;


import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.basho.proserv.datamigrator.Configuration;
import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.io.Key;


public class ThreadedClientDataReaderTests {

	@Test
	public void testThreadedRiakObjectReader() throws Exception {
		int TEST_SIZE = 1000000;
		
		Connection connection = new Connection();
		IClientReaderFactory factory = new DummyClientReaderFactory();
		Configuration config = new Configuration();
		config.setRiakWorkerCount(8);
				
		List<Key> dummyKeys = new ArrayList<Key>();
		for (Integer i = 0; i < TEST_SIZE; ++i) {
			dummyKeys.add(new Key("fakeBucket", i.toString()));
		}
		
		ThreadedClientDataReader reader = 
				new ThreadedClientDataReader(connection, 
						factory, 
						dummyKeys,
						config.getRiakWorkerCount(),
						config.getQueueSize());
		
		int readCount = 0;
		
		@SuppressWarnings("unused")
		Event event = Event.NULL;
		while (!(event = reader.readObject()).isNullEvent()) {
			++readCount;
			if (readCount == TEST_SIZE) {
				System.out.println("Test should be finished");
			}
		}
		
		assertTrue(readCount == TEST_SIZE);
	}
}
