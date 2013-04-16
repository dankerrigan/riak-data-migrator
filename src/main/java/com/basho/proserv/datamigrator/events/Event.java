package com.basho.proserv.datamigrator.events;

public abstract class Event {
	public static enum EventType { RIAK_OBJECT, KEY, IO_ERROR, VALUE_ERROR, NULL };
	public static Event NULL = new NullEvent();
	
	private final EventType eventType;
	
	public Event(EventType eventType) {
		this.eventType = eventType;
	}
	
	public EventType eventType() {
		return eventType;
	}
	
	public boolean isNullEvent() {
		return this.eventType == EventType.NULL;
	}
	
	public boolean isRiakObjectEvent() {
		return this.eventType == EventType.RIAK_OBJECT;
	}
	
	public boolean isKeyEvent() {
		return this.eventType == EventType.KEY;
	}
	
	public boolean isValueErrorEvent() {
		return this.eventType == EventType.VALUE_ERROR;
	}
	
	public boolean isIoErrorEvent() {
		return this.eventType == EventType.IO_ERROR;
	}
	
	public RiakObjectEvent asRiakObjectEvent() {
		if (!isRiakObjectEvent()) {
			throw new IllegalStateException("Event is not RiakObjectEvent");
		}
		
		return (RiakObjectEvent)this;
	}
	
	public KeyEvent asKeyEvent() {
		if (!isKeyEvent()) {
			throw new IllegalStateException("Event is not KeyEvent");
		}
		
		return (KeyEvent)this;
	}
	
	public ValueErrorEvent asValueErrorEvent() {
		if (!isValueErrorEvent()) {
			throw new IllegalStateException("Event is not ValueErrorEvent");
		}
		
		return (ValueErrorEvent)this;
	}
	
	public IoErrorEvent asIoErrorEvent() {
		if (!isIoErrorEvent()) {
			throw new IllegalStateException("Event is not IoErrorEvent");
		}
		
		return (IoErrorEvent)this;
	}
	
	public static class NullEvent extends Event{
		public NullEvent() {
			super(EventType.NULL);
		}
	}
	
}
