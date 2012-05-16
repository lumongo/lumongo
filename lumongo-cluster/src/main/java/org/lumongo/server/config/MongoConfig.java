package org.lumongo.server.config;

import java.io.File;
import java.io.IOException;

import org.lumongo.util.properties.PropertiesReader;
import org.lumongo.util.properties.PropertiesReader.PropertyException;

public class MongoConfig {
	
	public static final String DATABASE_NAME = "databaseName";
	public static final String MONGO_HOST = "mongoHost";
	public static final String MONGO_PORT = "mongoPort";
	
	public static MongoConfig getNodeConfig(File propertiesFile) throws IOException, PropertyException {
		PropertiesReader propertiesReader = new PropertiesReader(propertiesFile);
		return new MongoConfig(propertiesReader);
	}
	
	public static MongoConfig getNodeConfigFromClassPath(Class<?> askingClass, String propertiesFilePath) throws IOException, PropertyException {
		PropertiesReader propertiesReader = new PropertiesReader(askingClass, propertiesFilePath);
		return new MongoConfig(propertiesReader);
	}
	
	// mongo
	private String databaseName;
	private String mongoHost;
	private int mongoPort;
	
	public MongoConfig(String mongoHost, int mongoPort, String databaseName) {
		this.mongoHost = mongoHost;
		this.mongoPort = mongoPort;
		this.databaseName = databaseName;
	}
	
	public MongoConfig(PropertiesReader propertiesReader) throws PropertyException {
		
		mongoHost = propertiesReader.getString(MONGO_HOST);
		mongoPort = propertiesReader.getInteger(MONGO_PORT);
		databaseName = propertiesReader.getString(DATABASE_NAME);
		
	}
	
	public String getDatabaseName() {
		return databaseName;
	}
	
	public String getMongoHost() {
		return mongoHost;
	}
	
	public int getMongoPort() {
		return mongoPort;
	}
	
	@Override
	public String toString() {
		return "MongoConfig [databaseName=" + databaseName + ", mongoHost=" + mongoHost + ", mongoPort=" + mongoPort + "]";
	}
	
}
