package org.lumongo.admin;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.LumongoRestClient;
import org.lumongo.util.LogUtil;

import java.io.File;

public class FetchAssociated {

	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();

		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts(AdminConstants.ADDRESS).withRequiredArg().defaultsTo("localhost").describedAs("Lumongo server address");
		OptionSpec<Integer> restPortArg = parser.accepts(AdminConstants.REST_PORT).withRequiredArg().ofType(Integer.class)
						.defaultsTo(LumongoConstants.DEFAULT_REST_SERVICE_PORT).describedAs("Lumongo rest port");
		OptionSpec<String> idArg = parser.accepts(AdminConstants.ID).withRequiredArg().required().describedAs("Doc Id");
		OptionSpec<String> indexArg = parser.accepts(AdminConstants.INDEX).withRequiredArg().required().describedAs("Index");
		OptionSpec<String> fileNameArg = parser.accepts(AdminConstants.FILE_NAME).withRequiredArg().required().describedAs("Associated File Name");
		OptionSpec<File> outputFileArg = parser.accepts(AdminConstants.OUTPUT_FILE).withRequiredArg().ofType(File.class).required()
						.describedAs("Location to write associated file");

		try {
			OptionSet options = parser.parse(args);

			String address = options.valueOf(addressArg);
			int restPort = options.valueOf(restPortArg);
			String id = options.valueOf(idArg);
			String indexName = options.valueOf(indexArg);
			String fileName = options.valueOf(fileNameArg);
			File outputFile = options.valueOf(outputFileArg);

			LumongoRestClient client = new LumongoRestClient(address, restPort);
			client.fetchAssociated(id, indexName, fileName, outputFile);

		}
		catch (OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.formatHelpWith(new LumongoHelpFormatter());
			parser.printHelpOn(System.out);
		}
	}
}
