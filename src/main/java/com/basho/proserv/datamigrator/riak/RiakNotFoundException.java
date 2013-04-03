package com.basho.proserv.datamigrator.riak;

public class RiakNotFoundException extends Exception {
	private static final long serialVersionUID = 8714221229024734259L;
	private final String bucket;
	private final String key;
	
	public RiakNotFoundException(String bucket, String key) {
		this.bucket = bucket;
		this.key = key;
	}
	
	@Override
	public String toString() {
		return String.format("%s/%s not found", this.bucket, this.key);
	}
}
