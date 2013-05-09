package com.basho.proserv.datamigrator;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class Configuration {
	private static int RIAK_WORKER_MULTIPLIER = 2;
	private static int RIAK_CLUSTER_CONNECTION_MULTIPLIER = 4;
	private static int DEFAULT_RIAK_PB_PORT = 8087;
	private static int DEFAULT_RIAK_HTTP_PORT = 8098;
	private static int DEFAULT_QUEUE_SIZE = 1000;
	public static int MAX_RETRY = 3;
	public static int RETRY_WAIT_MILLIS = 10;
	public static enum Mode { LOAD, DUMP };
	public static enum Operation { ALL_BUCKETS, 
								   BUCKETS, 
								   ALL_KEYS, 
								   BUCKET_KEYS,
								   KEYS,
								   BUCKET_PROPERTIES,
								   DELETE_BUCKETS,
								   COPY_ALL,
								   COPY_BUCKETS };

	
	
	private Mode mode = Mode.LOAD;
	private Operation operation = Operation.ALL_BUCKETS;
	private boolean resume = false;
	private File filePath = new File("./");

	private Set<String> hosts = new HashSet<String>();
	private Set<String> destinationhosts = new HashSet<String>();
	
	private int port = DEFAULT_RIAK_PB_PORT;
	private int destinationPort = DEFAULT_RIAK_PB_PORT; 
	private int httpPort = DEFAULT_RIAK_HTTP_PORT;
	private int queueSize = DEFAULT_QUEUE_SIZE;
	
	private Set<String> bucketNames = new HashSet<String>();
	private Set<String> keyNames = new HashSet<String>();
	
	private boolean verboseStatus = true;
	private boolean resetVClock = false;
	
	private int riakWorkerCount = Runtime.getRuntime().availableProcessors() * RIAK_WORKER_MULTIPLIER;
	private int maxRiakConnections = riakWorkerCount * RIAK_CLUSTER_CONNECTION_MULTIPLIER;
	
	private String destinationBucket = null; 
	
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
	
	public boolean getResume() {
		return this.resume;
	}
	public void setResume(boolean resume) {
		this.resume = resume;
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
	
	public void addDestinationHost(String host) {
		this.destinationhosts.add(host);
	}
	public void addDestinationHosts(Collection<String> hosts) {
		this.destinationhosts.addAll(hosts);
	}
	public Set<String> getDestinationHosts() {
		return this.destinationhosts;
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
	
	public void setDestinationPort(int port) {
		this.destinationPort = port;
	}
	public int getDestinationPort() {
		return this.destinationPort;
	}
	
	public void setDestinationBucket(String destinationBucket) {
		this.destinationBucket = destinationBucket;
	}
	public String getDestinationBucket() {
		return this.destinationBucket;
	}
	
	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}
	public int getQueueSize() {
		return this.queueSize;
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

	public void addKeyNames(Collection<String> keys) {
		this.keyNames.addAll(keys);
	}
	public Set<String> getKeyNames() {
		return this.keyNames;
	}
	
	public void setVerboseStatus(boolean verboseStatus) {
		this.verboseStatus = verboseStatus;
	}
	public boolean getVerboseStatus() {
		return this.verboseStatus;
	}
	
	public void setResetVClock(boolean resetVClock) {
		this.resetVClock = resetVClock;
	}
	public boolean getResetVClock() {
		return this.resetVClock;
	}
	
	public void setRiakWorkerCount(int riakWorkerCount) {
		this.riakWorkerCount = riakWorkerCount;
		this.maxRiakConnections = riakWorkerCount * RIAK_CLUSTER_CONNECTION_MULTIPLIER;
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
