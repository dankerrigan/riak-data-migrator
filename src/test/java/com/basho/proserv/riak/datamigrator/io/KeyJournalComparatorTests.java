package com.basho.proserv.riak.datamigrator.io;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.io.KeyJournal;
import com.basho.proserv.datamigrator.io.UnwrittenKeyFinder;

public class KeyJournalComparatorTests {

	@Test
	public void test() throws Exception {
		String[] buckets = {"A", "B", "C", "D", "E", "F"};
		int KEY_COUNT = 100000;
		
		TemporaryFolder tempFolder = new TemporaryFolder();
		
		File keyFile = tempFolder.newFile();
		File keyFile2 = tempFolder.newFile();
		
		KeyJournal keyJournal = new KeyJournal(keyFile, KeyJournal.Mode.WRITE);
		KeyJournal keyJournal2 = new KeyJournal(keyFile2, KeyJournal.Mode.WRITE);
		
		for (String bucket : buckets) {
			for (Integer i = 0; i < KEY_COUNT; ++i) {
				keyJournal.write(bucket, i.toString());
				if (i % 2 == 0) {
					keyJournal2.write(bucket, i.toString());
				}
			}
		}
		
		keyJournal.close();
		keyJournal2.close();
		
		KeyJournal readKeyJournal = new KeyJournal(keyFile, KeyJournal.Mode.READ);
		KeyJournal readKeyJournal2 = new KeyJournal(keyFile2, KeyJournal.Mode.READ);
		
		UnwrittenKeyFinder comparator = new UnwrittenKeyFinder(readKeyJournal, readKeyJournal2);
		
		for (Key key : comparator) {
			assertTrue(Integer.parseInt(key.key()) % 2 != 0);
		}
		
		readKeyJournal.close();
		readKeyJournal2.close();
	}

}
