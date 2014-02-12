package com.basho.proserv.datamigrator;

import com.basho.proserv.datamigrator.io.AbstractKeyJournal;
import com.basho.proserv.datamigrator.io.IKeyJournal;
import com.basho.proserv.datamigrator.io.Key;
import com.basho.proserv.datamigrator.io.KeyJournal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class Utilities {
    public static Map<String, KeyJournal> splitKeys(File basePath, IKeyJournal keyJournal) {
        Map<String, KeyJournal> journals = new HashMap<String, KeyJournal>();
        Map<String, KeyJournal> readJournals = new HashMap<String, KeyJournal>();


        try {
            for (Key key : keyJournal) {
                String bucketName = key.bucket();
                if (!journals.containsKey(bucketName)) {
                    File bucketPath = new File(basePath.getAbsolutePath() + "/" + Utilities.urlEncode(bucketName));
                    bucketPath.mkdir();
                    File keyFile = new File(bucketPath.getAbsolutePath() + "/bucketkeys.keys");
                    journals.put(key.bucket(), new KeyJournal(keyFile, KeyJournal.Mode.WRITE));
                }

                journals.get(bucketName).write(key);
            }

            for (String bucketName: journals.keySet()) {
                journals.get(bucketName).close();
                File bucketKeys = new File(basePath.getAbsolutePath() + "/" + Utilities.urlEncode(bucketName) + "/" + "bucketkeys.keys");
                readJournals.put(bucketName, new KeyJournal(bucketKeys, KeyJournal.Mode.READ));
            }
        } catch (IOException ex) {
            return null;
        }
        return readJournals;
    }

	public static List<String> readFileLines(String filename) throws IOException, FileNotFoundException {
		List<String> lines = new ArrayList<String>();
		File file = new File(filename);
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		while((line = reader.readLine()) != null) {
			lines.add(line);
		}
		return lines;
	}
	
	public static Set<String> readUniqueFileLines(String filename) throws IOException, FileNotFoundException {
		List<String> lines = readFileLines(filename);
		Set<String> set = new HashSet<String>();
		
		for (String line : lines) {
			set.add(line);
		}
		
		return set;
	}
	
	public static String urlEncode(String input) {
		try {
			return java.net.URLEncoder.encode(input, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return input;
		}
	}
	
	public static String urlDecode(String input) {
		try {
			return java.net.URLDecoder.decode(input, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return input;
		}
	}

    public static List<String> urlDecode(Iterable<String> lines) {
        List<String> decoded = new ArrayList<String>();

        for (String line : lines) {
            decoded.add(urlDecode(line));
        }

        return decoded;
    }


}
