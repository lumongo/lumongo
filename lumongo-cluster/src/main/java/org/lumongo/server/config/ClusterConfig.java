package org.lumongo.server.config;

import org.bson.Document;
import org.lumongo.util.properties.PropertiesReader;
import org.lumongo.util.properties.PropertiesReader.PropertyException;

import java.io.File;
import java.io.IOException;

public class ClusterConfig {
	
	public static final String SHARDED = "sharded";
	public static final String INDEX_BLOCK_SIZE = "indexBlockSize";
	public static final String MAX_INDEX_BLOCKS = "maxIndexBlocks";
	public static final String MAX_INTERNAL_CLIENT_CONNECTIONS = "maxInternalClientConnections";
	public static final String INTERNAL_WORKERS = "internalWorkers";
	public static final String EXTERNAL_WORKERS = "externalWorkers";
	public static final String INTERNAL_SHUTDOWN_TIMEOUT = "internalShutdownTimeout";
	public static final String EXTERNAL_SHUTDOWN_TIMEOUT = "externalShutdownTimeout";
	
	public static ClusterConfig getClusterConfig(File propertiesFile) throws IOException, PropertyException {
		PropertiesReader propertiesReader = new PropertiesReader(propertiesFile);
		return new ClusterConfig(propertiesReader);
	}
	
	public static ClusterConfig getClusterConfigFromClassPath(Class<?> askingClass, String propertiesFilePath) throws IOException, PropertyException {
		PropertiesReader propertiesReader = new PropertiesReader(askingClass, propertiesFilePath);
		return new ClusterConfig(propertiesReader);
	}
	
	// mongo
	private boolean sharded;
	
	// general
	private int indexBlockSize;
	private int maxIndexBlocks;
	
	// sockets
	private int maxInternalClientConnections;
	private int internalWorkers;
	private int externalWorkers;
	
	// timeouts
	private int internalShutdownTimeout;
	private int externalShutdownTimeout;
	
	protected ClusterConfig() {
		
	}
	
	public ClusterConfig(PropertiesReader propertiesReader) throws PropertyException {
		
		sharded = propertiesReader.getBoolean(SHARDED);
		
		indexBlockSize = propertiesReader.getInteger(INDEX_BLOCK_SIZE);
		maxIndexBlocks = propertiesReader.getInteger(MAX_INDEX_BLOCKS);
		
		maxInternalClientConnections = propertiesReader.getInteger(MAX_INTERNAL_CLIENT_CONNECTIONS);
		internalWorkers = propertiesReader.getInteger(INTERNAL_WORKERS);
		externalWorkers = propertiesReader.getInteger(EXTERNAL_WORKERS);
		
		internalShutdownTimeout = propertiesReader.getInteger(INTERNAL_SHUTDOWN_TIMEOUT);
		externalShutdownTimeout = propertiesReader.getInteger(EXTERNAL_SHUTDOWN_TIMEOUT);
		
	}
	
	public boolean isSharded() {
		return sharded;
	}
	
	public int getIndexBlockSize() {
		return indexBlockSize;
	}
	
	public int getMaxIndexBlocks() {
		return maxIndexBlocks;
	}
	
	public int getMaxInternalClientConnections() {
		return maxInternalClientConnections;
	}
	
	public int getInternalWorkers() {
		return internalWorkers;
	}
	
	public int getExternalWorkers() {
		return externalWorkers;
	}
	
	public int getInternalShutdownTimeout() {
		return internalShutdownTimeout;
	}
	
	public int getExternalShutdownTimeout() {
		return externalShutdownTimeout;
	}
	
	public Document toDocument() {
		Document document = new Document();
		document.put(SHARDED, sharded);
		document.put(INDEX_BLOCK_SIZE, indexBlockSize);
		document.put(MAX_INDEX_BLOCKS, maxIndexBlocks);
		document.put(MAX_INTERNAL_CLIENT_CONNECTIONS, maxInternalClientConnections);
		document.put(INTERNAL_WORKERS, internalWorkers);
		document.put(EXTERNAL_WORKERS, externalWorkers);
		document.put(INTERNAL_SHUTDOWN_TIMEOUT, internalShutdownTimeout);
		document.put(EXTERNAL_SHUTDOWN_TIMEOUT, externalShutdownTimeout);
		return document;
		
	}
	
	public static ClusterConfig fromDBObject(Document settings) {
		ClusterConfig clusterConfig = new ClusterConfig();
		clusterConfig.sharded = (boolean) settings.get(SHARDED);
		clusterConfig.indexBlockSize = (int) settings.get(INDEX_BLOCK_SIZE);
		clusterConfig.maxIndexBlocks = (int) settings.get(MAX_INDEX_BLOCKS);
		clusterConfig.maxInternalClientConnections = (int) settings.get(MAX_INTERNAL_CLIENT_CONNECTIONS);
		clusterConfig.internalWorkers = (int) settings.get(INTERNAL_WORKERS);
		clusterConfig.externalWorkers = (int) settings.get(EXTERNAL_WORKERS);
		clusterConfig.internalShutdownTimeout = (int) settings.get(INTERNAL_SHUTDOWN_TIMEOUT);
		clusterConfig.externalShutdownTimeout = (int) settings.get(EXTERNAL_SHUTDOWN_TIMEOUT);
		return clusterConfig;
	}
	
	@Override
	public String toString() {
		return "ClusterConfig [sharded=" + sharded + ", indexBlockSize=" + indexBlockSize + ", maxIndexBlocks=" + maxIndexBlocks
						+ ", maxInternalClientConnections=" + maxInternalClientConnections + ", internalWorkers=" + internalWorkers + ", externalWorkers="
						+ externalWorkers + ", internalShutdownTimeout=" + internalShutdownTimeout + ", externalShutdownTimeout=" + externalShutdownTimeout
						+ "]";
	}
}
