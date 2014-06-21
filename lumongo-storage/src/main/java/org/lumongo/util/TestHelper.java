package org.lumongo.util;

import java.net.UnknownHostException;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class TestHelper {
	public static final String MONGO_SERVER_PROPERTY = "mongoServer";
	public static final String MONGO_PORT_PROPERTY = "mongoPort";
	public static final String TEST_DATABASE_NAME = "lumongoUnitTest";
	
	public static final String MONGO_SERVER_PROPERTY_DEFAULT = "127.0.0.1";
	public static final int MONGO_PORT_PROPERTY_DEFAULT = 27017;
	
	public static String getMongoServer() {
		String mongoServer = System.getProperty(MONGO_SERVER_PROPERTY);
		if (mongoServer == null) {
			return MONGO_SERVER_PROPERTY_DEFAULT;
		}
		return mongoServer;
	}
	
	public static int getMongoPort() {
		String portStr = System.getProperty(MONGO_PORT_PROPERTY);
		if (portStr == null) {
			return MONGO_PORT_PROPERTY_DEFAULT;
		}
		return Integer.parseInt(portStr);
	}
	
	public static MongoClient getMongo() throws UnknownHostException, MongoException {
		return new MongoClient(getMongoServer(), getMongoPort());
	}
}
