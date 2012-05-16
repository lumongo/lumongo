package org.lumongo.admin;

import java.io.File;
import java.util.Arrays;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.admin.help.RequiredOptionException;
import org.lumongo.client.LumongoClient;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.util.ClusterHelper;
import org.lumongo.util.LogUtil;
import org.lumongo.util.ServerNameHelper;

public class ClusterAdmin {
	private static final String MONGO_CONFIG = "mongoConfig";
	private static final String NODE_CONFIG = "nodeConfig";
	private static final String CLUSTER_CONFIG = "clusterConfig";
	private static final String SERVER = "server";
	private static final String HAZELCAST_PORT = "hazelcastPort";
	private static final String COMMAND = "command";
	
	public static enum Command {
		createCluster,
		updateCluster,
		removeCluster,
		showCluster,
		registerNode,
		removeNode,
		listNodes,
	}
	
	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();
		
		OptionParser parser = new OptionParser();
		OptionSpec<File> mongoConfigArg = parser.accepts(MONGO_CONFIG).withRequiredArg().ofType(File.class).describedAs("Mongo properties file");
		OptionSpec<File> nodeConfigArg = parser.accepts(NODE_CONFIG).withRequiredArg().ofType(File.class).describedAs("Node properties file");
		OptionSpec<File> clusterConfigArg = parser.accepts(CLUSTER_CONFIG).withRequiredArg().ofType(File.class).describedAs("Cluster properties file");
		OptionSpec<String> serverAddressArg = parser.accepts(SERVER).withRequiredArg().describedAs("Specific server address manually for node commands");
		OptionSpec<Integer> hazelcastPortArg = parser.accepts(HAZELCAST_PORT).withRequiredArg().ofType(Integer.class)
				.describedAs("Hazelcast port if multiple instances on one server for node commands");
		OptionSpec<Command> commandArg = parser.accepts(COMMAND).withRequiredArg().ofType(Command.class).required()
				.describedAs("Command to run " + Arrays.toString(Command.values()));
		
		LumongoClient client = null;
		
		try {
			OptionSet options = parser.parse(args);
			
			File mongoConfigFile = options.valueOf(mongoConfigArg);
			File nodeConfigFile = options.valueOf(nodeConfigArg);
			File clusterConfigFile = options.valueOf(clusterConfigArg);
			String serverAddress = options.valueOf(serverAddressArg);
			Integer hazelcastPort = options.valueOf(hazelcastPortArg);
			
			Command command = options.valueOf(commandArg);
			
			if (mongoConfigFile == null) {
				throw new RequiredOptionException(MONGO_CONFIG, command.toString());
			}
			
			MongoConfig mongoConfig = MongoConfig.getNodeConfig(mongoConfigFile);
			
			LocalNodeConfig localNodeConfig = null;
			if (nodeConfigFile != null) {
				localNodeConfig = LocalNodeConfig.getNodeConfig(nodeConfigFile);
			}
			
			ClusterConfig clusterConfig = null;
			if (clusterConfigFile != null) {
				clusterConfig = ClusterConfig.getClusterConfig(clusterConfigFile);
			}
			
			if (Command.createCluster.equals(command)) {
				if (clusterConfig == null) {
					throw new RequiredOptionException(CLUSTER_CONFIG, command.toString());
				}
				ClusterHelper.saveClusterConfig(mongoConfig, clusterConfig);
			}
			else if (Command.updateCluster.equals(command)) {
				if (clusterConfig == null) {
					throw new RequiredOptionException(CLUSTER_CONFIG, command.toString());
				}
				ClusterHelper.saveClusterConfig(mongoConfig, clusterConfig);
			}
			else if (Command.removeCluster.equals(command)) {
				ClusterHelper.removeClusterConfig(mongoConfig);
			}
			else if (Command.showCluster.equals(command)) {
				System.out.println(ClusterHelper.getClusterConfig(mongoConfig));
			}
			else if (Command.registerNode.equals(command)) {
				if (localNodeConfig == null) {
					throw new RequiredOptionException(NODE_CONFIG, command.toString());
				}
				if (serverAddress == null) {
					serverAddress = ServerNameHelper.getLocalServer();
					System.out.println("Using <" + serverAddress + "> as the server address.  If this is not correct please specify on command line");
					
				}
				
				ClusterHelper.registerNode(mongoConfig, localNodeConfig, serverAddress);
			}
			else if (Command.removeNode.equals(command)) {
				if (serverAddress == null) {
					serverAddress = ServerNameHelper.getLocalServer();
					System.out.println("Using <" + serverAddress + "> as the server address.  If this is not correct please specify on command line");
				}
				
				if (hazelcastPort == null) {
					hazelcastPort = LumongoConstants.DEFAULT_HAZELCAST_PORT;
				}
				
				ClusterHelper.removeNode(mongoConfig, serverAddress, hazelcastPort);
			}
			else if (Command.listNodes.equals(command)) {
				System.out.println(ClusterHelper.getNodes(mongoConfig));
			}
			else {
				System.err.println(command + " not supported");
			}
			
		}
		catch (OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.formatHelpWith(new LumongoHelpFormatter());
			parser.printHelpOn(System.err);
		}
		finally {
			if (client != null) {
				client.close();
			}
		}
	}
}
