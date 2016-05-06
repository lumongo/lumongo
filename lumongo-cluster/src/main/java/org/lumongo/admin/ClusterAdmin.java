package org.lumongo.admin;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.admin.help.RequiredOptionException;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.util.ClusterHelper;
import org.lumongo.util.LogUtil;
import org.lumongo.util.ServerNameHelper;

import java.io.File;
import java.util.Arrays;

public class ClusterAdmin {


	public enum Command {
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
		OptionSpec<File> mongoConfigArg = parser.accepts(AdminConstants.MONGO_CONFIG).withRequiredArg().ofType(File.class).describedAs("Mongo properties file");
		OptionSpec<File> nodeConfigArg = parser.accepts(AdminConstants.NODE_CONFIG).withRequiredArg().ofType(File.class).describedAs("Node properties file");
		OptionSpec<File> clusterConfigArg = parser.accepts(AdminConstants.CLUSTER_CONFIG).withRequiredArg().ofType(File.class).describedAs("Cluster properties file");
		OptionSpec<String> serverAddressArg = parser.accepts(AdminConstants.ADDRESS).withRequiredArg().describedAs("Specific server address manually for node commands");
		OptionSpec<Integer> hazelcastPortArg = parser.accepts(AdminConstants.HAZELCAST_PORT).withRequiredArg().ofType(Integer.class)
						.describedAs("Hazelcast port if multiple instances on one server for node commands");
		OptionSpec<Command> commandArg = parser.accepts(AdminConstants.COMMAND).withRequiredArg().ofType(Command.class).required()
						.describedAs("Command to run " + Arrays.toString(Command.values()));

		try {
			OptionSet options = parser.parse(args);

			File mongoConfigFile = options.valueOf(mongoConfigArg);
			File nodeConfigFile = options.valueOf(nodeConfigArg);
			File clusterConfigFile = options.valueOf(clusterConfigArg);
			String serverAddress = options.valueOf(serverAddressArg);
			Integer hazelcastPort = options.valueOf(hazelcastPortArg);

			Command command = options.valueOf(commandArg);

			if (mongoConfigFile == null) {
				throw new RequiredOptionException(AdminConstants.MONGO_CONFIG, command.toString());
			}

			MongoConfig mongoConfig = MongoConfig.getNodeConfig(mongoConfigFile);

			ClusterHelper clusterHelper = new ClusterHelper(mongoConfig);

			LocalNodeConfig localNodeConfig = null;
			if (nodeConfigFile != null) {
				localNodeConfig = LocalNodeConfig.getNodeConfig(nodeConfigFile);
			}

			ClusterConfig clusterConfig = null;
			if (clusterConfigFile != null) {
				clusterConfig = ClusterConfig.getClusterConfig(clusterConfigFile);
			}

			if (Command.createCluster.equals(command)) {
				System.out.println("Creating cluster in database <" + mongoConfig.getDatabaseName() + "> on mongo server <" + mongoConfig.getMongoHost() + ">");
				if (clusterConfig == null) {
					throw new RequiredOptionException(AdminConstants.CLUSTER_CONFIG, command.toString());
				}
				clusterHelper.saveClusterConfig(clusterConfig);
				System.out.println("Created cluster");
			}
			else if (Command.updateCluster.equals(command)) {
				System.out.println("Updating cluster in database <" + mongoConfig.getDatabaseName() + "> on mongo server <" + mongoConfig.getMongoHost() + ">");
				if (clusterConfig == null) {
					throw new RequiredOptionException(AdminConstants.CLUSTER_CONFIG, command.toString());
				}
				clusterHelper.saveClusterConfig(clusterConfig);
			}
			else if (Command.removeCluster.equals(command)) {
				System.out.println("Removing cluster from database <" + mongoConfig.getDatabaseName() + "> on mongo server <" + mongoConfig.getMongoHost()
								+ ">");
				clusterHelper.removeClusterConfig();
			}
			else if (Command.showCluster.equals(command)) {
				try {
					System.out.println(clusterHelper.getClusterConfig());
				}
				catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}
			else if (Command.registerNode.equals(command)) {
				if (localNodeConfig == null) {
					throw new RequiredOptionException(AdminConstants.NODE_CONFIG, command.toString());
				}
				if (serverAddress == null) {
					serverAddress = ServerNameHelper.getLocalServer();
				}
				if (hazelcastPort != null) {
					System.err.println("Set hazelcast port in node config file");
				}
				else {
					System.out.println("Registering node with server address <" + serverAddress + ">");

					clusterHelper.registerNode(localNodeConfig, serverAddress);
				}
			}
			else if (Command.removeNode.equals(command)) {
				if (serverAddress == null) {
					serverAddress = ServerNameHelper.getLocalServer();
				}

				if (hazelcastPort == null) {
					hazelcastPort = LumongoConstants.DEFAULT_HAZELCAST_PORT;
				}

				System.out.println("Removing node with server address <" + serverAddress + "> and hazelcastPort <" + hazelcastPort + ">");

				clusterHelper.removeNode(serverAddress, hazelcastPort);
			}
			else if (Command.listNodes.equals(command)) {
				System.out.println(clusterHelper.getNodes());
			}
			else {
				System.err.println(command + " not supported");
			}

		}
		catch (OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.formatHelpWith(new LumongoHelpFormatter());
			parser.printHelpOn(System.err);
			System.exit(2);
		}

	}
}
