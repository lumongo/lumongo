package org.lumongo.admin;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.LumongoClient;
import org.lumongo.client.config.LumongoClientConfig;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.util.BsonHelper;
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
			
			LumongoClientConfig lumongoClientConfig = new LumongoClientConfig();
			lumongoClientConfig.addMember(address, port);
			LumongoClient client = new LumongoClient(lumongoClientConfig);
			
			try {
				FetchResponse fr = client.fetchDocument(uniqueId);
				if (fr.hasResultDocument()) {
					ResultDocument rd = fr.getResultDocument();
					if (ResultDocument.Type.BSON.equals(rd.getType())) {
						DBObject dbObject = BsonHelper.dbObjectFromResultDocument(rd);
						System.out.println(dbObject.toString());
					}
					else if (ResultDocument.Type.TEXT.equals(rd.getType())) {
						System.out.println(rd.toByteString().toStringUtf8());
					}
					else {
						System.out.write(rd.toByteString().toByteArray());
						System.out.flush();
					}
				}
				else {
					System.err.println("ERROR: Document <" + uniqueId + "> is not found");
				}
			}
			finally {
				client.close();
			}
		}
		catch (OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.formatHelpWith(new LumongoHelpFormatter());
			parser.printHelpOn(System.err);
		}
	}
}
