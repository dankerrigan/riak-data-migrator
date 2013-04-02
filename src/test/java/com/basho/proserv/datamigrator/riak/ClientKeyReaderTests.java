package com.basho.proserv.datamigrator.riak;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.riak.client.IRiakObject;

public class ClientKeyReaderTests {

	@Test
	public void test() throws Exception {
		int TEST_SIZE = 1000000;
		
		Connection connection = new Connection();
		IClientReaderFactory factory = new DummyClientReaderFactory();
				
		List<Key> dummyKeys = new ArrayList<Key>();
		for (Integer i = 0; i < TEST_SIZE; ++i) {
			dummyKeys.add(new Key("dummyBucket", i.toString()));
		}
		
		ClientDataReader reader = 
				new ClientDataReader(connection, 
						factory, 
						dummyKeys);
		
		int readCount = 0;
		
		@SuppressWarnings("unused")
		IRiakObject object = null;
		while ((object = reader.readObject()) != null) {
			++readCount;
		}
		
		assertTrue(readCount == TEST_SIZE);
	}

}
