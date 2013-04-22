package com.basho.proserv.datamigrator.io;

import com.basho.proserv.datamigrator.events.RiakObjectEvent;

public interface IRiakObjectReader {
	public RiakObjectEvent readRiakObject();
	public void close();
}
