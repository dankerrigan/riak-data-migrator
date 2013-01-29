package com.basho.proserv.datamigrator;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class Configuration {
	private static int RIAK_WORKER_MULTIPLIER = 2;
	
	public static enum Mode { LOAD, DUMP };
	public static enum Operation { ALL_BUCKETS, BUCKETS, ALL_KEYS, BUCKET_KEYS };

	
	private Mode mode = Mode.LOAD;
	private Operation operation = Operation.ALL_BUCKETS;
	private File filePath = new File("./");

	private Set<String> hosts = new HashSet<String>();
	
	private int port = 8087;
	private int httpPort = 8098;
	
	private Set<String> bucketNames = new HashSet<String>();
	
	private boolean verboseStatus = false;
	
	private int riakWorkerCount = Runtime.getRuntime().availableProcessors() * RIAK_WORKER_MULTIPLIER;
	private int maxRiakConnections = riakWorkerCount * 4;
	
	public void setMode(Mode mode) {
		this.mode = mode;
	}
	public Mode getMode() {
		return this.mode;
	}
	
	public void setOperation(Operation operation) {
		this.operation = operation;
	}
	public Operation getOperation() {
		return this.operation;
	}
	
	public void setFilePath(File filePath) {
		this.filePath = filePath;
	}
	public File getFilePath() {
		return this.filePath;
	}
	
	public void addHost(String host) {
		this.hosts.add(host);
	}
	public void addHosts(Collection<String> hosts) {
		this.hosts.addAll(hosts);
	}
	public Set<String> getHosts() {
		return this.hosts;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	public int getPort() {
		return this.port;
	}
	
	public void setHttpPort(int port) {
		this.httpPort = port;
	}
	public int getHttpPort() {
		return this.httpPort;
	}
	
	public void addBucketName(String bucket) {
		this.bucketNames.add(bucket);
	}
	public void addBucketNames(Collection<String> buckets) {
		this.bucketNames.addAll(buckets);		
	}
	public Set<String> getBucketNames() {
		return this.bucketNames;
	}
	
	public void setVerboseStatus(boolean verboseStatus) {
		this.verboseStatus = verboseStatus;
	}
	public boolean getVerboseStatus() {
		return this.verboseStatus;
	}
	
	public void setRiakWorkerCount(int riakWorkerCount) {
		this.riakWorkerCount = riakWorkerCount;
		this.maxRiakConnections = riakWorkerCount * 4;
	}
	public int getRiakWorkerCount() {
		return this.riakWorkerCount;
	}
	
	public void setMaxRiakConnectionsCount(int maxRiakConnections) {
		this.maxRiakConnections = maxRiakConnections;
	}
	public int getMaxRiakConnections() {
		return this.maxRiakConnections;
	}
}
