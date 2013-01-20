package com.basho.proserv.datamigrator.io;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.pbc.RiakObject;
import com.basho.riak.pbc.RiakObjectIO;
import com.google.protobuf.InvalidProtocolBufferException;

public class RiakObjectReader {
	private final Logger log = LoggerFactory.getLogger(RiakObjectReader.class);
	private final RiakObjectIO riakObjectIo = new RiakObjectIO();
	private DataInputStream dataInputStream = null;
	private int errorCount = 0;
	
	public RiakObjectReader(File inputFile) {
		try {
			dataInputStream = new DataInputStream(new FileInputStream(inputFile));
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException("File could not be found " + inputFile.getAbsolutePath());
		}
		
	}
	
	public RiakObject readRiakObject() {
		try {
			return riakObjectIo.readRiakObject(this.dataInputStream);
		} catch (InvalidProtocolBufferException e) {
			log.error("readRiakObject protocol buffer exception", e);
			++this.errorCount;
		} catch (EOFException e) {
			//no-op, end of file reached
		} catch (IOException e) {
			log.error("readRiakObject IO exception", e);
			++this.errorCount;
		}
		return null;
	}
	
	public int errorCount() {
		return this.errorCount;
	}
	
	public void close() {
		try {
			this.dataInputStream.close();
		} catch (IOException e) {
			log.error("Could not close RiakObjectReader file");
		}
	}
}
