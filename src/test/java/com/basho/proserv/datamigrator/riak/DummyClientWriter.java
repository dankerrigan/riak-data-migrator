package com.basho.proserv.datamigrator.riak;

import com.basho.proserv.datamigrator.events.Event;
import com.basho.proserv.datamigrator.events.KeyEvent;
import com.basho.proserv.datamigrator.events.RiakObjectEvent;

public class DummyClientWriter implements IClientWriter {

	@Override
	public Event storeRiakObject(RiakObjectEvent riakObject) {
		return new KeyEvent(riakObject.key());
	}

	@Override
	public void setBucketRename(String bucketName) {
		// TODO Auto-generated method stub
		
	}

}
