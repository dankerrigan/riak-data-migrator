package com.basho.proserv.datamigrator.riak;

public class ClientReaderFactory implements IClientReaderFactory {
	
	public IClientReader createClientReader(Connection connection) {
		return new ClientReader(connection);
	}

}
