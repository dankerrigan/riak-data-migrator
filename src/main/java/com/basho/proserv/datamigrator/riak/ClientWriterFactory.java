package com.basho.proserv.datamigrator.riak;

public class ClientWriterFactory implements IClientWriterFactory {

	public IClientWriter createClientWriter(Connection connection) {
		return new ClientWriter(connection);
	}

}
