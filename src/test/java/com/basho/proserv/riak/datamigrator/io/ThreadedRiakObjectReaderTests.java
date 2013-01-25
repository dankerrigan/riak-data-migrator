package com.basho.proserv.riak.datamigrator.io;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.proserv.datamigrator.io.RiakObjectWriter;
import com.basho.proserv.datamigrator.io.ThreadedRiakObjectReader;
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
			writer.writeRiakObject(new RiakObject(ByteString.copyFromUtf8(""),
					ByteString.copyFromUtf8(""),
					ByteString.copyFromUtf8("")));
		}
		writer.close();
		
		ThreadedRiakObjectReader reader = new ThreadedRiakObjectReader(data);
		
		RiakObject object = null;
		int readCount = 0;
		while ((object = reader.readRiakObject()) != null) {
			++readCount;
		}
		
		assertTrue(readCount == RECORD_COUNT);
	}

}
