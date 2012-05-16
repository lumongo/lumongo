package org.lumongo.util;

import java.net.UnknownHostException;

import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class TestHelper {
	public static final String MONGO_SERVER_PROPERTY = "mongoServer";
	public static final String MONGO_PORT_PROPERTY = "mongoPort";
	public static final String TEST_DATABASE_NAME = "lumongoUnitTest";
	
	public static String getMongoServer() {
		String mongoServer = System.getProperty(MONGO_SERVER_PROPERTY);
		if (mongoServer == null) {
			throw new IllegalArgumentException(MONGO_SERVER_PROPERTY + " must be defined");
		}
		return mongoServer;
	}
	
	public static int getMongoPort() {
		String portStr = System.getProperty(MONGO_PORT_PROPERTY);
		if (portStr == null) {
			throw new IllegalArgumentException(MONGO_PORT_PROPERTY + " must be defined");
		}
		return Integer.parseInt(portStr);
	}
	
	public static Mongo getMongo() throws UnknownHostException, MongoException {
		return new Mongo(getMongoServer(), getMongoPort());
	}
}
