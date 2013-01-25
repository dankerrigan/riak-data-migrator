package com.basho.proserv.datamigrator.io;

public class Key {
	private final String bucket;
	private final String key;
	
	public Key(String bucket, String key) {
		this.bucket = bucket;
		this.key = key;
	}
	
	public boolean errorKey() {
		return this.key == null && this.bucket == null;
	}
	
	public static Key createErrorKey() {
		return new Key(null, null);
	}
	
	public String bucket() {
		return this.bucket;
	}
	
	public String key() {
		return this.key;
	}
	
}
