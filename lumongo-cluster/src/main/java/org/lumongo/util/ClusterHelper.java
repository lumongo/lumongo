package org.lumongo.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.server.config.Nodes;

import java.net.UnknownHostException;

public class ClusterHelper {
	
	public static final String CLUSTER_CONFIG = "cluster_config_";
	public static final String CLUSTER_MEMBERSHIP = "cluster_membership_";
	public static final String _ID = "_id";
	public static final String DATA = "data";
	public static final String CLUSTER = "cluster";
	public static final String INSTANCE = "instance";
	public static final String SERVER_ADDRESS = "serverAddress";
	
	//TODO dont reconnect to mongo constantly in this class
	
	public static void saveClusterConfig(MongoConfig mongoConfig, ClusterConfig clusterConfig) throws Exception {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());
		
		try {
			
			MongoDatabase db = mongo.getDatabase(mongoConfig.getDatabaseName());
			
			MongoCollection<Document> configCollection = db.getCollection(CLUSTER_CONFIG);

			Document query = new Document();
			query.put(_ID, CLUSTER);

			Document config = new Document();
			config.put(_ID, CLUSTER);
			config.put(DATA, clusterConfig.toDocument());

			configCollection.replaceOne(query, config, new UpdateOptions().upsert(true));
		}
		finally {
			mongo.close();
		}
		
	}
	
	public static void removeClusterConfig(MongoConfig mongoConfig) throws UnknownHostException, MongoException {
		
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());
		
		try {
			
			MongoDatabase db = mongo.getDatabase(mongoConfig.getDatabaseName());
			
			MongoCollection<Document> configCollection = db.getCollection(CLUSTER_CONFIG);
			
			Document search = new Document();
			search.put(_ID, CLUSTER);
			
			configCollection.deleteOne(search);
		}
		finally {
			mongo.close();
		}
	}
	
	public static ClusterConfig getClusterConfig(MongoConfig mongoConfig) throws Exception {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());
		
		try {
			
			MongoDatabase db = mongo.getDatabase(mongoConfig.getDatabaseName());
			
			MongoCollection<Document> configCollection = db.getCollection(CLUSTER_CONFIG);
			
			Document search = new Document();
			search.put(_ID, CLUSTER);
			
			Document result = configCollection.find(search).first();
			
			if (result == null) {
				throw new Exception("Create the cluster first using cluster admin tool");
			}
			else {
				Document object = (Document) result.get(DATA);
				return ClusterConfig.fromDBObject(object);
			}
		}
		finally {
			mongo.close();
		}
		
	}
	
	public static void registerNode(MongoConfig mongoConfig, LocalNodeConfig localNodeConfig, String serverAddress) throws Exception {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());
		
		try {
			
			MongoDatabase db = mongo.getDatabase(mongoConfig.getDatabaseName());

			MongoCollection<Document> membershipCollection = db.getCollection(CLUSTER_MEMBERSHIP);

			Document index = new Document();
			index.put(SERVER_ADDRESS, 1);
			index.put(INSTANCE, 1);

			IndexOptions options = new IndexOptions().unique(true);
			membershipCollection.createIndex(index, options);

			Document search = new Document();
			search.put(SERVER_ADDRESS, serverAddress);
			search.put(INSTANCE, localNodeConfig.getHazelcastPort());

			Document object = new Document();
			object.put(SERVER_ADDRESS, serverAddress);
			object.put(INSTANCE, localNodeConfig.getHazelcastPort());
			object.put(DATA, localNodeConfig.toDocument());

			membershipCollection.replaceOne(search, object, new UpdateOptions().upsert(true));
		}
		finally {
			mongo.close();
		}
		
	}
	
	public static void removeNode(MongoConfig mongoConfig, String serverAddress, int hazelcastPort) throws Exception {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());
		
		try {
			
			MongoDatabase db = mongo.getDatabase(mongoConfig.getDatabaseName());
			
			MongoCollection<Document> membershipCollection = db.getCollection(CLUSTER_MEMBERSHIP);
			
			Document search = new Document();
			search.put(SERVER_ADDRESS, serverAddress);
			search.put(INSTANCE, hazelcastPort);
			
			membershipCollection.deleteMany(search);
			
		}
		finally {
			mongo.close();
		}
		
	}
	
	public static LocalNodeConfig getNodeConfig(MongoConfig mongoConfig, String serverAddress, int instance) throws Exception {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());
		
		try {
			
			MongoDatabase db = mongo.getDatabase(mongoConfig.getDatabaseName());
			
			MongoCollection<Document> membershipCollection = db.getCollection(CLUSTER_MEMBERSHIP);
			
			Document search = new Document();
			search.put(SERVER_ADDRESS, serverAddress);
			search.put(INSTANCE, instance);
			
			Document result = membershipCollection.find(search).first();
			
			if (result == null) {
				throw new Exception("No node found with address <" + serverAddress + "> and hazelcast port <" + instance
								+ ">.  Please register the node with cluster admin tool");
			}
			
			Document dataObject = (Document) result.get(DATA);
			
			return LocalNodeConfig.fromDocument(dataObject);
			
		}
		finally {
			mongo.close();
		}
	}
	
	public static Nodes getNodes(MongoConfig mongoConfig) throws Exception {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());
		
		try {
			
			MongoDatabase db = mongo.getDatabase(mongoConfig.getDatabaseName());
			
			MongoCollection<Document> membershipCollection = db.getCollection(CLUSTER_MEMBERSHIP);
			
			FindIterable<Document> results = membershipCollection.find();
			
			Nodes nodes = new Nodes();
			
			for (Document object : results) {
				LocalNodeConfig lnc = LocalNodeConfig.fromDocument((Document) object.get(DATA));
				
				String serverAddress = (String) object.get(SERVER_ADDRESS);
				nodes.add(serverAddress, lnc);
			}
			
			return nodes;
		}
		finally {
			mongo.close();
		}
	}
}
