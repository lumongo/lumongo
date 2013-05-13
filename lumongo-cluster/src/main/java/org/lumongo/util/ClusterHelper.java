package org.lumongo.util;

import java.net.UnknownHostException;

import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.server.config.Nodes;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

public class ClusterHelper {

	public static final String MEMBERSHIP_INDEX = "membershipIndex";
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

		DB db = mongo.getDB(mongoConfig.getDatabaseName());

		DBCollection configCollection = db.getCollection(CLUSTER_CONFIG);

		BasicDBObject config = new BasicDBObject();
		config.put(_ID, CLUSTER);
		config.put(DATA, clusterConfig.toDBObject());

		configCollection.save(config, WriteConcern.SAFE);

	}

	public static void removeClusterConfig(MongoConfig mongoConfig) throws UnknownHostException, MongoException {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());

		DB db = mongo.getDB(mongoConfig.getDatabaseName());

		DBCollection configCollection = db.getCollection(CLUSTER_CONFIG);

		BasicDBObject search = new BasicDBObject();
		search.put(_ID, CLUSTER);

		configCollection.remove(search, WriteConcern.SAFE);
	}

	public static ClusterConfig getClusterConfig(MongoConfig mongoConfig) throws Exception {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());

		DB db = mongo.getDB(mongoConfig.getDatabaseName());

		DBCollection configCollection = db.getCollection(CLUSTER_CONFIG);

		BasicDBObject search = new BasicDBObject();
		search.put(_ID, CLUSTER);

		DBObject result = configCollection.findOne(search);

		if (result == null) {
			throw new Exception("Create the cluster first using cluster admin tool");
		}
		else {
			DBObject object = (DBObject) result.get(DATA);
			return ClusterConfig.fromDBObject(object);
		}

	}

	public static void registerNode(MongoConfig mongoConfig, LocalNodeConfig localNodeConfig, String serverAddress) throws Exception {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());

		DB db = mongo.getDB(mongoConfig.getDatabaseName());

		DBCollection membershipCollection = db.getCollection(CLUSTER_MEMBERSHIP);

		DBObject index = new BasicDBObject();
		index.put(SERVER_ADDRESS, 1);
		index.put(INSTANCE, 1);
		membershipCollection.ensureIndex(index, MEMBERSHIP_INDEX, true);

		BasicDBObject search = new BasicDBObject();
		search.put(SERVER_ADDRESS, serverAddress);
		search.put(INSTANCE, localNodeConfig.getHazelcastPort());

		BasicDBObject object = new BasicDBObject();
		object.put(SERVER_ADDRESS, serverAddress);
		object.put(INSTANCE, localNodeConfig.getHazelcastPort());
		object.put(DATA, localNodeConfig.toDBObject());

		membershipCollection.update(search, object, true, false, WriteConcern.SAFE);

	}

	public static void removeNode(MongoConfig mongoConfig, String serverAddress, int hazelcastPort) throws Exception {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());

		DB db = mongo.getDB(mongoConfig.getDatabaseName());

		DBCollection membershipCollection = db.getCollection(CLUSTER_MEMBERSHIP);

		BasicDBObject search = new BasicDBObject();
		search.put(SERVER_ADDRESS, serverAddress);
		search.put(INSTANCE, hazelcastPort);

		membershipCollection.remove(search, WriteConcern.SAFE);

	}

	public static LocalNodeConfig getNodeConfig(MongoConfig mongoConfig, String serverAddress, int instance) throws Exception {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());

		DB db = mongo.getDB(mongoConfig.getDatabaseName());

		DBCollection membershipCollection = db.getCollection(CLUSTER_MEMBERSHIP);

		BasicDBObject search = new BasicDBObject();
		search.put(SERVER_ADDRESS, serverAddress);
		search.put(INSTANCE, instance);

		DBObject result = membershipCollection.findOne(search);

		if (result == null) {
			throw new Exception("No node found with address <" + serverAddress + "> and hazelcast port <" + instance
					+ ">.  Please register the node with cluster admin tool");
		}

		DBObject dataObject = (DBObject) result.get(DATA);

		return LocalNodeConfig.fromDBObject(dataObject);
	}

	public static Nodes getNodes(MongoConfig mongoConfig) throws Exception {
		MongoClient mongo = new MongoClient(mongoConfig.getMongoHost(), mongoConfig.getMongoPort());

		DB db = mongo.getDB(mongoConfig.getDatabaseName());

		DBCollection membershipCollection = db.getCollection(CLUSTER_MEMBERSHIP);

		DBCursor results = membershipCollection.find();

		Nodes nodes = new Nodes();

		while (results.hasNext()) {
			DBObject object = results.next();
			LocalNodeConfig lnc = LocalNodeConfig.fromDBObject((DBObject) object.get(DATA));

			String serverAddress = (String) object.get(SERVER_ADDRESS);
			nodes.add(serverAddress, lnc);
		}

		return nodes;
	}

}
