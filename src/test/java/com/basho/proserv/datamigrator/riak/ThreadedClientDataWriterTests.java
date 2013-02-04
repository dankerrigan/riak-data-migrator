package com.basho.proserv.datamigrator.riak;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedClientDataWriterTests {

	@Test
	public void test() throws Exception {
		Connection connection = new Connection();
		int TEST_SIZE = 10000;
		IClientWriterFactory factory = new DummyClientWriterFactory();
		
		ByteString data = ByteString.copyFromUtf8("DATA");
		
		List<IRiakObject> dummyObjects = new ArrayList<IRiakObject>();
		for (Integer i = 0; i < TEST_SIZE; ++i) {
			dummyObjects.add(ConversionUtilWrapper.convertConcreteToInterface(new RiakObject(data, data, data, data)));
		}
		
		ThreadedClientDataWriter writer = new ThreadedClientDataWriter(connection,
				factory,
				dummyObjects);
	
		int writtenCount = 0;
		
		@SuppressWarnings("unused")
		IRiakObject riakObject = null;
		while ((riakObject = writer.writeObject()) != null) {
			++writtenCount;
		}
		
		System.out.println("Written Count: "  + writtenCount);
		assertTrue(writtenCount == TEST_SIZE);
	}
	

}
