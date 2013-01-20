package com.basho.proserv.riak.datamigrator.io;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.proserv.datamigrator.io.RiakObjectReader;
import com.basho.proserv.datamigrator.io.RiakObjectWriter;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class RiakObjectReaderWriterTests {

	
	
	@Test
	public void test() throws Exception {
		TemporaryFolder tempFolder = new TemporaryFolder();
		int objectCount = 5;
		
		RiakObject riakObject = new RiakObject(ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8(""),
				   ByteString.copyFromUtf8(""));
		
		File dataFile = tempFolder.newFile();
		RiakObjectWriter writer = new RiakObjectWriter(dataFile);
		
		for (int i = 0; i < objectCount; ++i) {
			writer.writeRiakObject(riakObject);
		}
		
		writer.close();
		
		RiakObjectReader reader = new RiakObjectReader(dataFile);
		RiakObject readRiakObject = null;
		
		int readCount = 0;
		while((readRiakObject = reader.readRiakObject()) != null) {
			++readCount;
		}
		
		assertTrue(readCount == objectCount);
		assertTrue(reader.errorCount() == 0);
		
		reader.close();
	}

}
