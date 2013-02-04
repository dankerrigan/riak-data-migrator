package com.basho.proserv.riak.datamigrator.io;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.io.KeyJournal;
import com.basho.proserv.datamigrator.io.RiakObjectWriter;
import com.basho.proserv.datamigrator.io.ThreadedRiakObjectReader;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class ThreadedRiakObjectReaderTests {

	@Test
	public void test() throws Exception {
		int RECORD_COUNT = 10000;
		TemporaryFolder tempFolder = new TemporaryFolder();
		File data = tempFolder.newFile();
		
		RiakObjectWriter writer = new RiakObjectWriter(data);
		
		for (int i = 0; i < RECORD_COUNT; ++i) {
			IRiakObject riakObject = ConversionUtilWrapper.convertConcreteToInterface(
					new RiakObject(ByteString.copyFromUtf8(""),
					ByteString.copyFromUtf8("Bucket"),
					ByteString.copyFromUtf8("Key"),
					ByteString.copyFromUtf8("")));
			writer.writeRiakObject(riakObject);
		}
		writer.close();
		
		ThreadedRiakObjectReader reader = new ThreadedRiakObjectReader(data, false);
		
		
		IRiakObject object = null;
		int readCount = 0;
		while ((object = reader.readRiakObject()) != null) {
			++readCount;	
		}
		
		reader.close();
		
		assertTrue(readCount == RECORD_COUNT);
		
	}

}
