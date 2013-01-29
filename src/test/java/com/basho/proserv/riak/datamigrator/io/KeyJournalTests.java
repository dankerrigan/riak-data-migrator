package com.basho.proserv.riak.datamigrator.io;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.io.KeyJournal;

public class KeyJournalTests {

	@Test
	public void test() throws Exception {
		int KEY_COUNT = 1000;
		TemporaryFolder tempFolder = new TemporaryFolder();
		
		File keyPath = tempFolder.newFile();
		
		String[] buckets = {"A", "B", "C", "D", "E", "F"};
		String[] keys = new String[KEY_COUNT];
		for (Integer i = 0; i < keys.length; ++i) {
			keys[i] = i.toString();
		}
		
		KeyJournal journal = new KeyJournal(keyPath, KeyJournal.Mode.WRITE);
		
		for (String bucket : buckets) {
			for (String key : keys) {
				journal.write(bucket, key);
			}
		}
		journal.close();
		
		KeyJournal readJournal = new KeyJournal(keyPath, KeyJournal.Mode.READ);
		
		int readCount = 0;
		for (Key key : readJournal) {
			if (!key.errorKey()) 
				++readCount;
		}
		
		assertTrue(readCount == buckets.length * keys.length);
	}
	
	@Test
	public void testCreateKeyPathFromPath() {
		File file = new File("/Users/dankerrigan/data.data");
		File newPath = KeyJournal.createKeyPathFromPath(file, false);
		assertTrue(newPath.getAbsolutePath().compareTo("/Users/dankerrigan/data.keys") == 0);
		newPath = KeyJournal.createKeyPathFromPath(file, true);
		assertTrue(newPath.getAbsolutePath().compareTo("/Users/dankerrigan/data.loadedkeys") == 0);
	}
	

}
