package com.basho.riak.pbc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.InvalidProtocolBufferException;

public class RiakObjectIO implements RiakMessageCodes {
	@SuppressWarnings("unused")
	private final Logger log = LoggerFactory.getLogger(RiakObjectIO.class);

	
	public RiakObjectIO() {
		//no-op
	}
	
	public boolean writeRiakObject(DataOutputStream outputStream, RiakObject riakObject) throws IOException {
		return this.writeRiakObject(outputStream, riakObject, MSG_PutReq);
	}
	
	// Uses code liberated from com.basho.riak.pbc.RiakClient.store
	public boolean writeRiakObject(DataOutputStream outputStream, RiakObject riakObject, int code) throws IOException {
		RiakKvPB.RpbPutReq.Builder builder = RiakKvPB.RpbPutReq.newBuilder()
				.setBucket(riakObject.getBucketBS())
				.setKey(riakObject.getKeyBS())
				.setContent(riakObject.buildContent());

		if (riakObject.getVclock() != null) {
			builder.setVclock(riakObject.getVclock());
		}

		builder.setReturnBody(false);

		RiakKvPB.RpbPutReq req = builder.build();
		int len = req.getSerializedSize();
		
		outputStream.writeInt(len + 1);
		outputStream.write(code);
		req.writeTo(outputStream);
		
		return true;
	}
	
	// Uses code liberated from com.basho.riak.pbc.RiakConnection.receive(byte[] data)
	// using message code of 255 to indicate end of file
	private byte[] receive(DataInputStream din, int code) throws IOException {
		int len;
		int get_code;
		byte[] data = null;


		len = din.readInt();
		get_code = din.read();

		if (len > 1) {
			data = new byte[len - 1];
			din.readFully(data);
		}

		if (get_code == 255) {
			throw new EOFException();
		}
		
		if (code != get_code) {
			throw new IOException("bad message code. Expected: " + code + " actual: " + get_code);
		}

		return data;
	}
	
	// Uses code liberated from com.basho.riak.pbc.RiakClient.fetch...
	private RiakObject processRiakObject(byte[] rep) 
			throws InvalidProtocolBufferException {
		
		RiakKvPB.RpbPutReq request = RiakKvPB.RpbPutReq.parseFrom(rep);
	    
	    RiakObject out = new RiakObject(request.getVclock(), request.getBucket(), request.getKey(), request.getContent());
	    
	    return out; 
	}
	
	
	
	public RiakObject readRiakObject(DataInputStream din) 
			throws IOException, InvalidProtocolBufferException {
		return processRiakObject(receive(din, MSG_PutReq));
	}


}
