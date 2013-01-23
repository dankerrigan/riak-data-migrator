package com.basho.proserv.datamigrator.riak;

public class ClientReaderFactory implements IClientReaderFactory {
	
	@Override
	public IClientReader createClientReader(Connection connection) {
		return new ClientReader(connection);
	}

}
