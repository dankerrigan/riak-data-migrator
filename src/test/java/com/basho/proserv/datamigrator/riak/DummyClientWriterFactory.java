package com.basho.proserv.datamigrator.riak;

public class DummyClientWriterFactory implements IClientWriterFactory {

	@Override
	public IClientWriter createClientWriter(Connection connection) {
		return new DummyClientWriter();
	}

}
