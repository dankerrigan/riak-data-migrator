package com.basho.proserv.datamigrator.riak;

import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterClientFactory;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.basho.riak.client.raw.http.HTTPRiakClientFactory;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterClientFactory;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.client.raw.pbc.PBRiakClientFactory;

public class Connection {
	private Logger log = LoggerFactory.getLogger(Connection.class);
	
	private static enum ClientType { HTTP, PB }
	
	private final static int DEFAULT_MAX_CONNECTIONS = 50;
	
	private int maxConnections = DEFAULT_MAX_CONNECTIONS;

	public RawClient riakClient = null;
	
	public Connection() {
		this(DEFAULT_MAX_CONNECTIONS);
	}
	
	public Connection(int maxConnections) {
		this.maxConnections = maxConnections;
	}
	
	public boolean connected() {
		return riakClient == null ? false : true;
	}
	
	public boolean testConnection() {
		if (this.connected()) {
			try {
				this.riakClient.ping();
				return true;
			} catch (IOException e) {
				//no-op
			}
		}
		return false;
	}
	
	
	public PBClusterConfig createPbClusterConfig(Set<String> hosts, int port) {
		PBClusterConfig pbClusterConfig = new PBClusterConfig(this.maxConnections);
		for (String host : hosts) {
			PBClientConfig config = createPBClientConfig(host, port);
			pbClusterConfig.addClient(config);
		}
		
		return pbClusterConfig;
	}
	
	public HTTPClusterConfig createHttpClusterConfig(Set<String> hosts, int port) {
		HTTPClusterConfig httpClusterConfig = new HTTPClusterConfig(this.maxConnections);
		for (String host : hosts) {
			HTTPClientConfig config = createHTTPClientConfig(host, port);
			httpClusterConfig.addClient(config);
		}
		return httpClusterConfig;
	}
	
	public boolean connectHTTPCluster(Set<String> hosts, int port) {
		boolean success = false;
		HTTPClusterConfig clusterConfig = createHttpClusterConfig(hosts, port);
		try {
			this.riakClient = HTTPClusterClientFactory.getInstance().newClient(clusterConfig);
			success = true;
		} catch (IOException e) {
			log.error("Could not create new HTTP Cluster Client.", e);
		}
		return success;
	}
	public boolean connectPBCluster(Set<String> hosts, int port) {
		boolean success = false;
		PBClusterConfig clusterConfig = createPbClusterConfig(hosts, port);
		try {
			this.riakClient = PBClusterClientFactory.getInstance().newClient(clusterConfig);
			success = true;
		} catch (IOException e) {
			log.error("Could not create new PB Cluster Client.", e);
		}
		return success;
	}
	
	public boolean connectCluster (ClientType clientType, Set<String> hosts, int port) {
		if (clientType == ClientType.HTTP) {
			return connectHTTPCluster(hosts, port);
		} else if (clientType == ClientType.PB) {
			return connectPBCluster(hosts, port);
		} else {
			return false;
		}
	}
	
	public boolean connectPBClient(String host, Integer port) {
		boolean success = false;
		PBClientConfig clientConfig = createPBClientConfig(host, port);
		try {
			this.riakClient = PBRiakClientFactory.getInstance().newClient(clientConfig);
			success = true;
		} catch (IOException e) {
			log.error("Could not create new PB Client", e);
		}
		return success;
	}
	
	public boolean connectHTTPClient(String host, Integer port) {
		boolean success = false;
		HTTPClientConfig clientConfig = createHTTPClientConfig(host, port);
		
		this.riakClient = HTTPRiakClientFactory.getInstance().newClient(clientConfig);
		success = true;
		
		return success;
	}

	public static PBClientConfig createPBClientConfig(String host, Integer port) {
		PBClientConfig.Builder b = new PBClientConfig.Builder();
		
		b.withHost(host);
		if (port != null) {
			b.withPort(port);
		}
		b.withRequestTimeoutMillis(0);
		
		return b.build();
	}
	
	public static HTTPClientConfig createHTTPClientConfig(String host, Integer port) {
		HTTPClientConfig.Builder b = new HTTPClientConfig.Builder();
		
		b.withHost(host);
		if (port != null) {
			b.withPort(port);
		}
		
		return b.build();
	}
	
	public boolean close() {
		boolean success = false;
		
		if (connected()) {
			this.riakClient.shutdown();
			success = true;
			this.riakClient = null;
		}
		
		return success;
	}
}
