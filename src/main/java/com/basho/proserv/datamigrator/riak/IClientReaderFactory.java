package com.basho.proserv.datamigrator.riak;

public interface IClientReaderFactory {
	IClientReader createClientReader(Connection connection);
}
