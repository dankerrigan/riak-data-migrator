package com.basho.proserv.datamigrator.riak;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.basho.riak.pbc.RiakObject;

public class ClientKeyReaderTests {

	@Test
	public void test() throws Exception {
		int TEST_SIZE = 1000000;
		
		Connection connection = new Connection();
		IClientReaderFactory factory = new DummyClientReaderFactory();
				
		List<String> dummyKeys = new ArrayList<String>();
		for (Integer i = 0; i < TEST_SIZE; ++i) {
			dummyKeys.add(i.toString());
		}
		
		ClientDataReader reader = 
				new ClientDataReader(connection, 
						factory, 
						"fakeBucket", 
						dummyKeys);
		
		int readCount = 0;
		
		@SuppressWarnings("unused")
		RiakObject object = null;
		while ((object = reader.readObject()) != null) {
			++readCount;
		}
		
		assertTrue(readCount == TEST_SIZE);
	}

}
