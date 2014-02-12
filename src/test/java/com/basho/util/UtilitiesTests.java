package com.basho.util;

import com.basho.proserv.datamigrator.Utilities;
import com.basho.proserv.datamigrator.io.AbstractKeyJournal;
import com.basho.proserv.datamigrator.io.IKeyJournal;
import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.io.KeyJournal;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Created with IntelliJ IDEA.
 * User: dankerrigan
 * Date: 1/29/14
 * Time: 9:01 AM
 * To change this template use File | Settings | File Templates.
 */
public class UtilitiesTests {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();


    @Test
    public void testSplitKeys() throws Exception {
        int KEY_COUNT = 10;
        File keyPath = tempFolder.newFile("data.keys");
        KeyJournal writeKeyJournal = new KeyJournal(keyPath, KeyJournal.Mode.WRITE);

        String[] buckets = new String[] {"A", "B", "C", "D", "E", "F"};
        for (String bucket : buckets) {
            for (Integer i = 0; i < KEY_COUNT; ++i) {
                writeKeyJournal.write(new Key(bucket, i.toString()));
            }
        }

        writeKeyJournal.close();

        KeyJournal readKeyJournal = new KeyJournal(keyPath, KeyJournal.Mode.READ);

        File splitFolder = tempFolder.newFolder();

        Map<String, KeyJournal> readJournals = Utilities.splitKeys(splitFolder, readKeyJournal);

        String[] splitDirs = splitFolder.list();

        int actualBucketCount = splitDirs.length;

        for (String splitDir : splitDirs) {
            File newBucketKeyPath = new File(splitFolder.getAbsolutePath() + "/" +  splitDir + "/" + "bucketkeys.keys");

            KeyJournal newJournal = new KeyJournal(newBucketKeyPath, KeyJournal.Mode.READ);

            Iterator<Key> iter = newJournal.iterator();
            for (Integer i = 0; i < KEY_COUNT; ++i) {
                assertTrue(i.toString().compareTo(iter.next().key()) == 0);
            }
        }

        assertTrue(actualBucketCount * KEY_COUNT== (buckets.length * KEY_COUNT));


        for (String bucketName : readJournals.keySet()) {
            Set<String> keys = new HashSet<String>();
            for (Key key : readJournals.get(bucketName)) {
                keys.add(key.key());
            }
            assertEquals(keys.size(), KEY_COUNT);
        }
    }
}
