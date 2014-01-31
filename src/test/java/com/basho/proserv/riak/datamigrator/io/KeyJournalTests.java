package com.basho.proserv.riak.datamigrator.io;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import com.basho.proserv.datamigrator.io.BucketKeyJournal;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.io.KeyJournal;

public class KeyJournalTests {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void test() throws Exception {
		int KEY_COUNT = 1000;
		
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
    public void testBucketKeyJournal() throws Exception {
        int KEY_COUNT = 1000;
        String TEST_BUCKET = "test_bucket";

        File keyPath = tempFolder.newFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(keyPath));

        String[] keys = new String[KEY_COUNT];
        for (Integer i = 0; i < keys.length; ++i) {
            keys[i] = i.toString();
            writer.write(keys[i] + '\n');
        }

        writer.flush();
        writer.close();

        BucketKeyJournal readJournal = new BucketKeyJournal(keyPath, KeyJournal.Mode.READ, TEST_BUCKET);

        int readCount = 0;
        for (Key key : readJournal) {
            assertTrue(key.bucket().compareTo(TEST_BUCKET) == 0);
            assertTrue(keys[readCount].compareTo(key.key()) == 0);
            if (!key.errorKey())
                ++readCount;
        }

        assertTrue(readCount == keys.length);
    }
	
	@Test
	public void testCreateKeyPathFromPath() throws Exception {
		File file = tempFolder.newFile("data.data");
		File newPath = KeyJournal.createKeyPathFromPath(file, false);
		assertTrue(newPath.getName().compareTo("data.keys") == 0);
		newPath = KeyJournal.createKeyPathFromPath(file, true);
		assertTrue(newPath.getName().compareTo("data.loadedkeys") == 0);
	}
	

}
