package com.basho.proserv.datamigrator.events;

import com.basho.riak.client.IRiakObject;

public class RiakObjectEvent extends Event {

	final private IRiakObject[] riakObjects;
	
	public RiakObjectEvent(IRiakObject[] riakObjects) {	
		super(Event.EventType.RIAK_OBJECT);
		
		this.riakObjects = riakObjects;
	}
	
	public IRiakObject[] riakObjects() {
		return this.riakObjects;
	}

}
