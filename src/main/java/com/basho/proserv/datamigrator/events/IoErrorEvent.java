package com.basho.proserv.datamigrator.events;

import java.io.IOException;

import com.basho.proserv.datamigrator.io.Key;

public class IoErrorEvent extends Event {

	final private Key key;
	final private IOException ioException;
	
	public IoErrorEvent(Key key, IOException ioException) {
		super(Event.EventType.IO_ERROR);
		
		this.key = key;
		this.ioException = ioException;
	}

	public Key key() {
		return this.key;
	}
	
	public IOException ioException() {
		return this.ioException;
	}
}
