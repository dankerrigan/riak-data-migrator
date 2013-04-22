package com.basho.proserv.datamigrator.events;

import com.basho.proserv.datamigrator.io.Key;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.pbc.ConversionUtilWrapper;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

public class RiakObjectEvent extends Event {

	final public static RiakObjectEvent STOP = new StopRiakObjectEvent();
	final public static RiakObjectEvent NULL = new NullRiakObjectEvent();
	
	final private IRiakObject[] riakObjects;
	
	public RiakObjectEvent(IRiakObject riakObject) {
		this(new IRiakObject[] { riakObject });
		
		if (riakObject == null) {
			throw new IllegalArgumentException("riakObject cannot be null");
		}
	}
	
	public RiakObjectEvent(IRiakObject[] riakObjects) {	
		super(Event.EventType.RIAK_OBJECT);
		if (riakObjects == null) {
			throw new IllegalArgumentException("riakObject cannot be null");
		}
		if (riakObjects.length < 1) {
			throw new IllegalArgumentException("lenght of riakObject must be greater than 0");
		}
		
		this.riakObjects = riakObjects;
	}
	
	public IRiakObject[] riakObjects() {
		return this.riakObjects;
	}
	
	public int count() {
		return this.riakObjects.length;
	}
	
	public Key key() {
		IRiakObject object = this.riakObjects[0];
		return new Key(object.getBucket(), object.getKey());
	}
	
	public long dataSize() {
		long acc = 0;
		for (int i = 0; i < this.riakObjects.length; ++i) {
			acc += this.riakObjects[i].getValue().length;
		}
		return acc;
	}
	
	public boolean isStopEvent() {
		return this == STOP;
	}
	
	public boolean isNullEvent() {
		return this == NULL;
	}
	
	public static class StopRiakObjectEvent extends RiakObjectEvent {
		private final static String STOP_FLAG_STRING = "STOP";
		private final static ByteString STOP_FLAG = ByteString.copyFromUtf8(STOP_FLAG_STRING);
		private static IRiakObject STOP_OBJECT = ConversionUtilWrapper.convertConcreteToInterface(
				new RiakObject(STOP_FLAG, STOP_FLAG, STOP_FLAG, STOP_FLAG));
		
		public StopRiakObjectEvent() {
			super(STOP_OBJECT);
		}
	}
	public static class NullRiakObjectEvent extends RiakObjectEvent {
		private final static String NULL_FLAG_STRING = "NULL";
		private final static ByteString NULL_FLAG = ByteString.copyFromUtf8(NULL_FLAG_STRING);
		private static IRiakObject STOP_OBJECT = ConversionUtilWrapper.convertConcreteToInterface(
				new RiakObject(NULL_FLAG, NULL_FLAG, NULL_FLAG, NULL_FLAG));
		
		public NullRiakObjectEvent() {
			super(STOP_OBJECT);
		}
	}

}
