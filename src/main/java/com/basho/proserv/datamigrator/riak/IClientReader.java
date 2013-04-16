package com.basho.proserv.datamigrator.riak;

import com.basho.proserv.datamigrator.events.Event;

public interface IClientReader {
	public Event fetchRiakObject(String bucket, String key) 
			throws InterruptedException;
}
