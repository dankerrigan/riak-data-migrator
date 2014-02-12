package com.basho.proserv.datamigrator;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.proserv.datamigrator.riak.Connection;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;

public class BucketDumperLoaderTests {
    @Rule
    public TemporaryFolder tempFolderMaker = new TemporaryFolder();

	private final int DEFAULT_WORKER_COUNT = Runtime.getRuntime().availableProcessors() * 2;
	private String host = "127.0.0.1";
	private int port = 8087;
	private int httpPort = 8098;
	
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
		Connection httpConnection = new Connection();
		connection.connectPBClient(host, port);
		httpConnection.connectHTTPClient(host, httpPort);
		int count = loadTestData(connection);
		System.out.println("Loaded " + count + " records");

		dumpDirectory = tempFolderMaker.newFolder();
		
		Configuration config = new Configuration();
		config.setFilePath(dumpDirectory);
		config.setRiakWorkerCount(DEFAULT_WORKER_COUNT);
		BucketDumper dumper = new BucketDumper(connection, httpConnection, config);
		
		long dumpCount = dumper.dumpAllBuckets(false, false);
		
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
		Connection httpConnection = new Connection();
		connection.connectPBClient(host, port);
		httpConnection.connectHTTPClient(host, httpPort);
		
		int deleteCount = deleteTestData(connection);
		System.out.println("Deleted " + deleteCount + " records");
		
		Configuration config = new Configuration();
		config.setFilePath(dumpDirectory);
		config.setRiakWorkerCount(DEFAULT_WORKER_COUNT);
		
		BucketLoader loader = new BucketLoader(connection, httpConnection, config);
		
		long loadCount = loader.LoadAllBuckets();
		
		System.out.println("Loaded " + loadCount + " records");
		
		assertTrue(deleteCount == loadCount);
		assertTrue(loader.errorCount() == 0);
		
		connection.close();
	}

}
