package com.basho.proserv.datamigrator.riak;

public class DummyClientReaderFactory implements IClientReaderFactory {

	@Override
	public IClientReader createClientReader(Connection connection) {
		return new DummyClientReader();
	}

}
