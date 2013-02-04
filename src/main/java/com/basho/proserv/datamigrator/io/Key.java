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
	
	@Override
	public boolean equals(Object other) {
		boolean result = false;
		if (other instanceof Key) {
			Key otherKey = (Key)other;
			result = otherKey.key().compareTo(this.key) == 0 &&
					otherKey.bucket().compareTo(this.bucket) == 0;
		}
		return result;
	}
	
	@Override
	public int hashCode() {
		if (!errorKey()) {
			return 41 * (41 + (this.key.hashCode() + this.bucket.hashCode()));
		} else {
			return 0;
		}
	}
}
