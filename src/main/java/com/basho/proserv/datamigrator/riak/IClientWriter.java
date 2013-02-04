package com.basho.proserv.datamigrator.riak;

import java.io.IOException;

import com.basho.riak.client.IRiakObject;

public interface IClientWriter {
	public IRiakObject storeRiakObject(IRiakObject riakObject) throws IOException;
}
