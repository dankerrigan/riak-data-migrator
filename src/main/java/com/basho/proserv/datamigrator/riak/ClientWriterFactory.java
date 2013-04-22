package com.basho.proserv.datamigrator.riak;

public class ClientWriterFactory implements IClientWriterFactory {

	private String bucketRename = null;
	
	@Override
	public IClientWriter createClientWriter(Connection connection) {
		ClientWriter writer = new ClientWriter(connection);
		
		if (this.bucketRename != null) {
			writer.setBucketRename(this.bucketRename);
		}
		
		return writer;
	}

	@Override
	public void setBucketRename(String bucketName) {
		this.bucketRename = bucketName;
	}
}
