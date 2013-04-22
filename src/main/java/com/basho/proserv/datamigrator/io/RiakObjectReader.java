package com.basho.proserv.datamigrator.io;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.proserv.datamigrator.events.RiakObjectEvent;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.pbc.RiakObject;
import com.basho.riak.pbc.RiakObjectIO;
import com.google.protobuf.InvalidProtocolBufferException;

public class RiakObjectReader implements IRiakObjectReader{
	private final Logger log = LoggerFactory.getLogger(RiakObjectReader.class);
	private final RiakObjectIO riakObjectIo = new RiakObjectIO();
	private DataInputStream dataInputStream = null;
	private final boolean resetVClock;
	private int errorCount = 0;
	
	public RiakObjectReader(File inputFile, boolean resetVClock) {
		this.resetVClock = resetVClock;
		try {
			dataInputStream = new DataInputStream(
					new GZIPInputStream(new BufferedInputStream(new FileInputStream(inputFile))));
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException("File could not be found " + inputFile.getAbsolutePath());
		} catch (IOException e) {
			throw new IllegalArgumentException("Could not open file " + inputFile.getAbsolutePath());
		}
	}
	
	public RiakObjectEvent readRiakObject() {
		try {
			RiakObject riakObject = riakObjectIo.readRiakObject(this.dataInputStream);
			IRiakObject object = ConversionUtilWrapper.convertConcreteToInterface(riakObject);
			if (this.resetVClock) {
				RiakObjectBuilder builder = 
						RiakObjectBuilder.newBuilder(riakObject.getBucket(), riakObject.getKey());
//				builder.withVClock(object.getVClock()); //stripping vclock
		        builder.withContentType(object.getContentType());
//				builder.withLastModified(object.getLastModified().getTime()); // no preserved in pbc conversion
		        builder.withValue(object.getValue());
		        builder.withLinks(object.getLinks());
		        builder.withIndexes(new RiakIndexes(object.allBinIndexes(), object.allIntIndexesV2()));//object.allIntIndexes()));
		        builder.withUsermeta(object.getMeta());
		        IRiakObject newObject = builder.build();
		        return new RiakObjectEvent(newObject);
			} else {
				return new RiakObjectEvent(object);
			}
		} catch (InvalidProtocolBufferException e) {
			log.error("readRiakObject protocol buffer exception", e);
			++this.errorCount;
		} catch (EOFException e) {
			//no-op, end of file reached
		} catch (IOException e) {
			log.error("readRiakObject IO exception", e);
			++this.errorCount;
		}
		return RiakObjectEvent.NULL;
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
