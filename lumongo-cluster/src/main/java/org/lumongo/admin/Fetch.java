package org.lumongo.admin;

import com.mongodb.DBObject;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoBaseWorkPool;
import org.lumongo.client.pool.LumongoPool;
import org.lumongo.client.result.FetchResult;
import org.lumongo.util.LogUtil;

import java.util.List;

public class Fetch {
	
	public static void main(String[] args) throws Exception {
		
		LogUtil.loadLogConfig();
		
		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts(AdminConstants.ADDRESS).withRequiredArg().defaultsTo("localhost").describedAs("Lumongo server address");
		OptionSpec<Integer> portArg = parser.accepts(AdminConstants.PORT).withRequiredArg().ofType(Integer.class)
						.defaultsTo(LumongoConstants.DEFAULT_EXTERNAL_SERVICE_PORT).describedAs("Lumongo external port");
		OptionSpec<String> uniqueIdArg = parser.accepts(AdminConstants.UNIQUE_ID).withRequiredArg().required().describedAs("Unique Id to fetch");
		OptionSpec<String> indexArg = parser.accepts(AdminConstants.INDEX).withRequiredArg().required().describedAs("Index to fetch from");
		
		OptionSpec<String> documentFieldsArg = parser.accepts(AdminConstants.DOCUMENT_FIELDS, "Fields to return from mongo").withRequiredArg();
		OptionSpec<String> documentMaskedFieldsArg = parser.accepts(AdminConstants.DOCUMENT_MASKED_FIELDS, "Fields to mask from mongo").withRequiredArg();
		
		try {
			OptionSet options = parser.parse(args);
			
			String address = options.valueOf(addressArg);
			int port = options.valueOf(portArg);
			String uniqueId = options.valueOf(uniqueIdArg);
			String indexName = options.valueOf(indexArg);
			
			List<String> documentFields = options.valuesOf(documentFieldsArg);
			List<String> documentMaskedFields = options.valuesOf(documentMaskedFieldsArg);
			
			LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
			lumongoPoolConfig.addMember(address, port);
			LumongoBaseWorkPool lumongoWorkPool = new LumongoBaseWorkPool(new LumongoPool(lumongoPoolConfig));
			
			try {
				
				FetchDocument fetch = new FetchDocument(uniqueId, indexName);
				
				for (String documentField : documentFields) {
					fetch.addDocumentField(documentField);
				}
				
				for (String documentMaskedField : documentMaskedFields) {
					fetch.addDocumentMaskedField(documentMaskedField);
				}
				
				FetchResult fr = lumongoWorkPool.execute(fetch);
				if (fr.hasResultDocument()) {
					DBObject dbObject = fr.getDocument();
					System.out.println(dbObject.toString());
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
