package com.basho.proserv.datamigrator.events;

import com.basho.proserv.datamigrator.io.Key;

public class ValueErrorEvent extends Event {

	final private Key key;
	
	public ValueErrorEvent(Key key) {
		super(Event.EventType.VALUE_ERROR);
		
		this.key = key;
	}
	
	public Key key() {
		return this.key;
	}

}
