package com.basho.proserv.datamigrator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Utilities {
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
}
