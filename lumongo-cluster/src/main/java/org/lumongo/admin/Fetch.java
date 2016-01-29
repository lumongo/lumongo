package org.lumongo.admin;

import com.google.protobuf.ServiceException;
import com.mongodb.DBObject;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.FetchResult;
import org.lumongo.util.LogUtil;

import java.io.IOException;
import java.util.List;

public class Fetch {

	public static void main(String[] args) throws Exception {

		LogUtil.loadLogConfig();

		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts(AdminConstants.ADDRESS).withRequiredArg().defaultsTo("localhost").describedAs("Lumongo server address");
		OptionSpec<Integer> portArg = parser.accepts(AdminConstants.PORT).withRequiredArg().ofType(Integer.class)
				.defaultsTo(LumongoConstants.DEFAULT_EXTERNAL_SERVICE_PORT).describedAs("Lumongo external port");
		OptionSpec<String> uniqueIdArg = parser.accepts(AdminConstants.ID).withRequiredArg().required().describedAs("Unique Id to fetch");
		OptionSpec<String> indexArg = parser.accepts(AdminConstants.INDEX).withRequiredArg().required().describedAs("Index to fetch from");

		OptionSpec<String> documentFieldsArg = parser.accepts(AdminConstants.DOCUMENT_FIELDS, "Fields to return from mongo").withRequiredArg();
		OptionSpec<String> documentMaskedFieldsArg = parser.accepts(AdminConstants.DOCUMENT_MASKED_FIELDS, "Fields to mask from mongo").withRequiredArg();

		int exitCode = 0;
		LumongoWorkPool lumongoWorkPool = null;
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
			lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);

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
			exitCode = 2;
		}
		catch (ServiceException | IOException e) {
			System.err.println("ERROR: " + e.getMessage());
			exitCode = 1;
		}
		finally {
			if (lumongoWorkPool != null) {
				lumongoWorkPool.shutdown();
			}
		}

		System.exit(exitCode);
	}
}
