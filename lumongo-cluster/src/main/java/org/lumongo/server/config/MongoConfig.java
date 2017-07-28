package org.lumongo.server.config;

import com.google.common.base.Splitter;
import com.mongodb.ServerAddress;
import org.lumongo.util.properties.PropertiesReader;
import org.lumongo.util.properties.PropertiesReader.PropertyException;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MongoConfig {

	public static final String DATABASE_NAME = "databaseName";
	public static final String MONGO_HOSTS = "mongoHosts";

	public static final Splitter semiSplitter = Splitter.on(";");

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
	private String mongoHosts;

	public MongoConfig(String mongoHosts, String databaseName) {
		this.mongoHosts = mongoHosts;
		this.databaseName = databaseName;
	}

	public MongoConfig(PropertiesReader propertiesReader) throws PropertyException {

		mongoHosts = propertiesReader.getString(MONGO_HOSTS);
		databaseName = propertiesReader.getString(DATABASE_NAME);

	}

	public String getDatabaseName() {
		return databaseName;
	}

	public List<ServerAddress> getServerAddresses() {

		return semiSplitter.splitToList(mongoHosts).stream().map(ServerAddress::new).collect(Collectors.toList());
	}

	@Override
	public String toString() {
		return "MongoConfig [databaseName=" + databaseName + ", mongoHosts=" + mongoHosts + "]";
	}

}
