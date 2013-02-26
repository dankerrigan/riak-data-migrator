package com.basho.proserv.datamigrator.riak;

public class DummyClientReaderFactory implements IClientReaderFactory {

	public IClientReader createClientReader(Connection connection) {
		return new DummyClientReader();
	}

}
