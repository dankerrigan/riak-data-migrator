package com.basho.proserv.riak.datamigrator.io;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.proserv.datamigrator.io.KeyJournal;
import com.basho.proserv.datamigrator.io.RiakObjectReader;
import com.basho.proserv.datamigrator.io.RiakObjectWriter;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
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
		File keys = tempFolder.newFile();
		RiakObjectWriter writer = new RiakObjectWriter(dataFile);
		
		for (int i = 0; i < objectCount; ++i) {
			writer.writeRiakObject(riakObject);
		}
		
		writer.close();
		
		RiakObjectReader reader = new RiakObjectReader(dataFile);
		@SuppressWarnings("unused")
		RiakObject readRiakObject = null;
		
		int readCount = 0;
		while((readRiakObject = reader.readRiakObject()) != null) {
			++readCount;
		}
		
		assertTrue(readCount == objectCount);
		assertTrue(reader.errorCount() == 0);
		
		reader.close();
	}

	@Test 
	public void testMetadataProperties() throws Exception {
		TemporaryFolder tempFolder = new TemporaryFolder();
		
		
		RiakObjectBuilder builder = RiakObjectBuilder.newBuilder("Test", "Test");
		builder.addIndex("index1", 1);
		builder.addIndex("index2", "index2");
		builder.addLink("linkedBucket", "linkedKey", "linkedTag");
		builder.addUsermeta("metaKey", "metaValue");
		builder.withContentType("content-type");
//		builder.withLastModified(1234567890);
		builder.withValue("value");
		builder.withVClock("1234567890".getBytes());
//		builder.withVtag("175xDv0I3UFCfGRC7K7U9z");
		
		IRiakObject iRiakObject = builder.build();
		RiakObject riakObject = ConversionUtilWrapper.convertInterfaceToConcrete(iRiakObject);
		
		File dataFile = tempFolder.newFile();
		File keys = tempFolder.newFile();
		RiakObjectWriter writer = new RiakObjectWriter(dataFile);
		
		writer.writeRiakObject(riakObject);
		
		writer.close();
		
		RiakObjectReader reader = new RiakObjectReader(dataFile);
		RiakObject readRiakObject = reader.readRiakObject();
		IRiakObject readObject = ConversionUtilWrapper.convertConcreteToInterface(readRiakObject);
		
		Set<Integer> intIndex = readObject.getIntIndex("index1");
		Set<String> strIndex = readObject.getBinIndex("index2");
		List<RiakLink> links = readObject.getLinks();
		String userMeta = readObject.getUsermeta("metaKey");
		String contentType = readObject.getContentType();
//		Date lastModified = readObject.getLastModified();
		String vClock = readObject.getVClockAsString();
		String value = readObject.getValueAsString();
//		String vTag = readObject.getVtag();
		
		assertTrue(intIndex.toArray(new Integer[1])[0] == 1);
		assertTrue(strIndex.toArray(new String[1])[0].compareTo("index2") == 0);
		RiakLink link = links.get(0);
		assertTrue(link.getBucket().compareTo("linkedBucket") == 0);
		assertTrue(link.getKey().compareTo("linkedKey") == 0);
		assertTrue(link.getTag().compareTo("linkedTag") == 0);
		assertTrue(userMeta.compareTo("metaValue") == 0);
		assertTrue(contentType.split(";")[0].trim().compareTo("content-type") == 0);
		//last modified not carried
		assertTrue(value.compareTo("value")==0);
		assertTrue(vClock.compareTo("1234567890") == 0);
		//assertTrue(vTag.compareTo("175xDv0I3UFCfGRC7K7U9z")==0);
		
		reader.close();
	}
}
