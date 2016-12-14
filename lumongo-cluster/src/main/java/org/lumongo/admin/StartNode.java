package org.lumongo.admin;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.lucene.search.BooleanQuery;
import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.server.LumongoNode;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.util.LogUtil;
import org.lumongo.util.ServerNameHelper;

import java.io.File;

public class StartNode {

	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();

		OptionParser parser = new OptionParser();
		OptionSpec<File> mongoConfigArg = parser.accepts(AdminConstants.MONGO_CONFIG).withRequiredArg().ofType(File.class).describedAs("Mongo properties file")
						.required();
		OptionSpec<String> serverAddressArg = parser.accepts(AdminConstants.ADDRESS).withRequiredArg().describedAs("Specific Server Address Manually");
		OptionSpec<Integer> hazelcastPortArg = parser.accepts(AdminConstants.HAZELCAST_PORT).withRequiredArg().ofType(Integer.class)
						.describedAs("Hazelcast port if multiple instances on one server (expert)");


		try {
			OptionSet options = parser.parse(args);

			File mongoConfigFile = options.valueOf(mongoConfigArg);
			String serverAddress = options.valueOf(serverAddressArg);
			Integer hazelcastPort = options.valueOf(hazelcastPortArg);

			MongoConfig mongoConfig = MongoConfig.getNodeConfig(mongoConfigFile);

			if (serverAddress == null) {
				serverAddress = ServerNameHelper.getLocalServer();
				System.out.println("Using <" + serverAddress + "> as the server address.  If this is not correct please specify on command line");
			}

			if (hazelcastPort == null) {
				hazelcastPort = LumongoConstants.DEFAULT_HAZELCAST_PORT;
			}

			BooleanQuery.setMaxClauseCount(16 * 1024);

			LumongoNode luceneNode = new LumongoNode(mongoConfig, serverAddress, hazelcastPort);

			luceneNode.start();
			luceneNode.setupShutdownHook();

		}
		catch (OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.formatHelpWith(new LumongoHelpFormatter());
			parser.printHelpOn(System.err);
			System.exit(2);
		}

	}
}
