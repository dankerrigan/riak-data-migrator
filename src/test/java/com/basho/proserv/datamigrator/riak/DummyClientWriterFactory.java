package com.basho.proserv.datamigrator.riak;

public class DummyClientWriterFactory implements IClientWriterFactory {

	public IClientWriter createClientWriter(Connection connection) {
		return new DummyClientWriter();
	}

}
