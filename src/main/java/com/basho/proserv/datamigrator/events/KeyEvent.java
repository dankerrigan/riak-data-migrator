package com.basho.proserv.datamigrator.events;

import com.basho.proserv.datamigrator.io.Key;

public class KeyEvent extends Event {
	
	final private Key key;

	public KeyEvent(Key key) {
		super(Event.EventType.KEY);
		
		this.key = key;
	}
	
	public Key key() {
		return this.key;
	}

}
