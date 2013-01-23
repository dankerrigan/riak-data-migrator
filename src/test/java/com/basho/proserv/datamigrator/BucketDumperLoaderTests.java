package com.basho.proserv.datamigrator;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;

public class BucketDumperLoaderTests {

	private String host = "127.0.0.1";
	private int port = 18087;
	
	File dumpDirectory = null;
	
	public static String[] bucketNames = {"A","B","C","D","E","F"};
	public static String[] keys = new String[100]; 
	
	static {
		for (Integer i = 0; i < keys.length; ++i) {
			keys[i] = i.toString();
		}
	}
	
	private int loadTestData(Connection connection) throws Exception{
		int count = 0;
		byte[] data = "1234567890".getBytes();
		for (String bucketName : bucketNames) {
			for (String key : keys) {
				IRiakObject riakObject = RiakObjectBuilder.newBuilder(bucketName, key).withValue(data).build();
				connection.riakClient.store(riakObject);
				++count;
			}
		}
		return count;
	}
	
	private int deleteTestData(Connection connection) throws Exception {
		int count = 0;
		for (String bucketName : bucketNames) {
			for (String key : keys) {
				connection.riakClient.delete(bucketName, key);
				++count;
			}
		}
		return count;
	}
	
	@Test
	public void testDump() throws Exception {
		Connection connection = new Connection();
		connection.connectPBClient(host, port);
		int count = loadTestData(connection);
		System.out.println("Loaded " + count + " records");
		
		TemporaryFolder tempFolderMaker = new TemporaryFolder();
		dumpDirectory = tempFolderMaker.newFolder();
		BucketDumper dumper = new BucketDumper(connection, dumpDirectory, false);
		
		int dumpCount = dumper.dumpAllBuckets();
		
		System.out.println("Dumped " + dumpCount + " with " + dumper.errorCount() + " errors");
		
		assertTrue(dumpCount == bucketNames.length * keys.length);
		assertTrue(dumpDirectory.list().length == bucketNames.length);
		assertTrue(dumper.errorCount() == 0);
				
		connection.close();
	}
	
	@Test 
	public void testLoad() throws Exception {
		testDump();
		Connection connection = new Connection();
		connection.connectPBClient(host, port);
		
		int deleteCount = deleteTestData(connection);
		System.out.println("Deleted " + deleteCount + " records");
		
		BucketLoader loader = new BucketLoader(connection, dumpDirectory, false);
		
		int loadCount = loader.LoadAllBuckets();
		
		System.out.println("Loaded " + loadCount + " records");
		
		assertTrue(deleteCount == loadCount);
		assertTrue(loader.errorCount() == 0);
		
		connection.close();
	}

}
