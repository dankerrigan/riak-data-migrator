package com.basho.proserv.datamigrator.riak;

import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.events.RiakObjectEvent;

public interface IClientWriter {
	public Event storeRiakObject(RiakObjectEvent object);
	public void setBucketRename(String bucketName);
}
