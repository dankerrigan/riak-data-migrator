package com.basho.proserv.datamigrator.io;

import com.basho.proserv.datamigrator.events.RiakObjectEvent;

public interface IRiakObjectWriter {
	
	public boolean writeRiakObject(RiakObjectEvent riakObject);
	public void close();

}
