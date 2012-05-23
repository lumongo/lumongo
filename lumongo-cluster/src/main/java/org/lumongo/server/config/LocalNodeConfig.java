package org.lumongo.server.config;

import java.io.File;
import java.io.IOException;

import org.lumongo.util.properties.PropertiesReader;
import org.lumongo.util.properties.PropertiesReader.PropertyException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class LocalNodeConfig {
	
	public static final String HAZELCAST_PORT = "hazelcastPort";
	public static final String INTERNAL_SERVICE_PORT = "internalServicePort";
	public static final String EXTERNAL_SERVICE_PORT = "externalServicePort";
	public static final String REST_PORT = "restPort";
	
	public static LocalNodeConfig getNodeConfig(File propertiesFile) throws IOException, PropertyException {
		PropertiesReader propertiesReader = new PropertiesReader(propertiesFile);
		return new LocalNodeConfig(propertiesReader);
	}
	
	public static LocalNodeConfig getNodeConfigFromClassPath(Class<?> askingClass, String propertiesFilePath) throws IOException, PropertyException {
		PropertiesReader propertiesReader = new PropertiesReader(askingClass, propertiesFilePath);
		return new LocalNodeConfig(propertiesReader);
	}
	
	// hazelcast
	private int hazelcastPort;
	
	// socket
	private int internalServicePort;
	private int externalServicePort;
	
	private int restPort;
	
	protected LocalNodeConfig() {
		restPort = -1;
	}
	
	public LocalNodeConfig(PropertiesReader propertiesReader) throws PropertyException {
		this();
		hazelcastPort = propertiesReader.getInteger(HAZELCAST_PORT);
		internalServicePort = propertiesReader.getInteger(INTERNAL_SERVICE_PORT);
		externalServicePort = propertiesReader.getInteger(EXTERNAL_SERVICE_PORT);
		if (propertiesReader.hasKey(REST_PORT)) {
			restPort = propertiesReader.getInteger(REST_PORT);
		}
	}
	
	public int getHazelcastPort() {
		return hazelcastPort;
	}
	
	public int getInternalServicePort() {
		return internalServicePort;
	}
	
	public int getExternalServicePort() {
		return externalServicePort;
	}
	
	public int getRestPort() {
		return restPort;
	}
	
	public DBObject toDBObject() {
		DBObject dbObject = new BasicDBObject();
		dbObject.put(HAZELCAST_PORT, hazelcastPort);
		dbObject.put(INTERNAL_SERVICE_PORT, internalServicePort);
		dbObject.put(EXTERNAL_SERVICE_PORT, externalServicePort);
		dbObject.put(REST_PORT, restPort);
		return dbObject;
		
	}
	
	public boolean hasRestPort() {
		return (restPort != -1);
	}
	
	public static LocalNodeConfig fromDBObject(DBObject settings) {
		LocalNodeConfig localNodeConfig = new LocalNodeConfig();
		localNodeConfig.hazelcastPort = (int) settings.get(HAZELCAST_PORT);
		localNodeConfig.internalServicePort = (int) settings.get(INTERNAL_SERVICE_PORT);
		localNodeConfig.externalServicePort = (int) settings.get(EXTERNAL_SERVICE_PORT);
		if (settings.containsField(REST_PORT)) {
			localNodeConfig.restPort = (int) settings.get(REST_PORT);
		}
		
		return localNodeConfig;
	}
	
	@Override
	public String toString() {
		return "LocalNodeConfig [hazelcastPort=" + hazelcastPort + ", internalServicePort=" + internalServicePort + ", externalServicePort="
				+ externalServicePort + ", restPort=" + restPort + "]";
	}
	
}
