package com.basho.proserv.datamigrator.riak;


import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.riak.pbc.RiakObject;


public class ThreadedClientDataReaderTests {

	@Test
	public void testThreadedRiakObjectReader() throws Exception {
		int TEST_SIZE = 1000000;
		
		Connection connection = new Connection();
		IClientReaderFactory factory = new DummyClientReaderFactory();
				
		List<Key> dummyKeys = new ArrayList<Key>();
		for (Integer i = 0; i < TEST_SIZE; ++i) {
			dummyKeys.add(new Key("fakeBucket", i.toString()));
		}
		
		ThreadedClientDataReader reader = 
				new ThreadedClientDataReader(connection, 
						factory, 
						dummyKeys,
						8);
		
		int readCount = 0;
		
		@SuppressWarnings("unused")
		RiakObject object = null;
		while ((object = reader.readObject()) != null) {
			++readCount;
		}
		
		assertTrue(readCount == TEST_SIZE);
	}
}
