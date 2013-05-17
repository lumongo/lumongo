package org.lumongo.client.config;

import java.util.ArrayList;
import java.util.List;

import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo.LMMember;

public class LumongoPoolConfig {

	private List<LMMember> members;

	private int maxConnections;
	private int maxIdle;
	private int defaultRetries;
	private String poolName;
	private boolean compressedConnection;
	private boolean routingEnabled;

	public final static int DEFAULT_DEFAULT_RETRIES = 0;

	public LumongoPoolConfig() {
		this.members = new ArrayList<LMMember>();
		this.maxConnections = 16;
		this.maxIdle = 16;
		this.defaultRetries = DEFAULT_DEFAULT_RETRIES;
		this.poolName = null;
		this.compressedConnection = false;
		this.routingEnabled = true;
	}

	public LumongoPoolConfig addMember(String serverAddress) {
		return addMember(serverAddress, LumongoConstants.DEFAULT_EXTERNAL_SERVICE_PORT, LumongoConstants.DEFAULT_REST_SERVICE_PORT);
	}


	public LumongoPoolConfig addMember(String serverAddress, int externalPort) {
		return addMember(serverAddress, externalPort, LumongoConstants.DEFAULT_REST_SERVICE_PORT);
	}


	public LumongoPoolConfig addMember(String serverAddress, int externalPort, int restPort) {
		LMMember member = LMMember.newBuilder().setServerAddress(serverAddress).setExternalPort(externalPort).setRestPort(restPort).build();
		members.add(member);
		return this;
	}


	public LumongoPoolConfig addMember(LMMember member) {
		members.add(member);
		return this;
	}


	public LumongoPoolConfig clearMembers() {
		members.clear();
		return this;
	}

	public List<LMMember> getMembers() {
		return members;
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	public LumongoPoolConfig setMaxConnections(int maxConnections) {
		this.maxConnections = maxConnections;
		return this;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public LumongoPoolConfig setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
		return this;
	}

	public int getDefaultRetries() {
		return defaultRetries;
	}

	public LumongoPoolConfig setDefaultRetries(int defaultRetries) {
		this.defaultRetries = defaultRetries;
		return this;
	}


	public String getPoolName() {
		return poolName;
	}


	public LumongoPoolConfig setPoolName(String poolName) {
		this.poolName = poolName;
		return this;
	}

	public boolean isCompressedConnection() {
		return compressedConnection;
	}

	public void setCompressedConnection(boolean compressedConnection) {
		this.compressedConnection = compressedConnection;
	}

	public boolean isRoutingEnabled() {
		return routingEnabled;
	}

	public void setRoutingEnabled(boolean routingEnabled) {
		this.routingEnabled = routingEnabled;
	}

}
