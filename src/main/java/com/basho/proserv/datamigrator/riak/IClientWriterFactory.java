package com.basho.proserv.datamigrator.riak;

interface IClientWriterFactory {
	IClientWriter createClientWriter(Connection connection);
	void setBucketRename(String bucketName);

}
