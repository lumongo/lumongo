package org.lumongo.admin;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoPool;
import org.lumongo.client.pool.LumongoBaseWorkPool;
import org.lumongo.client.result.FetchResult;
import org.lumongo.util.LogUtil;

import com.mongodb.DBObject;

public class Fetch {

	public static void main(String[] args) throws Exception {

		LogUtil.loadLogConfig();

		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts(AdminConstants.ADDRESS).withRequiredArg().defaultsTo("localhost").describedAs("Lumongo server address");
		OptionSpec<Integer> portArg = parser.accepts(AdminConstants.PORT).withRequiredArg().ofType(Integer.class)
				.defaultsTo(LumongoConstants.DEFAULT_EXTERNAL_SERVICE_PORT).describedAs("Lumongo external port");
		OptionSpec<String> uniqueIdArg = parser.accepts(AdminConstants.UNIQUE_ID).withRequiredArg().required().describedAs("Unique Id to fetch");

		try {
			OptionSet options = parser.parse(args);

			String address = options.valueOf(addressArg);
			int port = options.valueOf(portArg);
			String uniqueId = options.valueOf(uniqueIdArg);

			LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
			lumongoPoolConfig.addMember(address, port);
			LumongoBaseWorkPool lumongoWorkPool = new LumongoBaseWorkPool(new LumongoPool(lumongoPoolConfig));

			try {

				FetchResult fr = lumongoWorkPool.execute(new FetchDocument(uniqueId));
				if (fr.hasResultDocument()) {
					if (fr.isDocumentBson()) {
						DBObject dbObject = fr.getDocumentAsBson();
						System.out.println(dbObject.toString());
					}
					else if (fr.isDocumentText()) {
						System.out.println(fr.getDocumentAsUtf8());
					}
					else {
						System.out.write(fr.getDocumentAsBytes());
						System.out.flush();
					}
				}
				else {
					System.err.println("ERROR: Document <" + uniqueId + "> is not found");
				}
			}
			finally {
				lumongoWorkPool.shutdown();
			}
		}
		catch (OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.formatHelpWith(new LumongoHelpFormatter());
			parser.printHelpOn(System.err);
		}
	}
}
