package org.lumongo.admin;

import java.io.File;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.LumongoRestClient;
import org.lumongo.util.LogUtil;

public class FetchAssociated {
	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();
		
		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts("address").withRequiredArg().defaultsTo("localhost").describedAs("Lumongo server address");
		OptionSpec<Integer> restPortArg = parser.accepts("restPort").withRequiredArg().ofType(Integer.class).defaultsTo(LumongoConstants.REST_SERVICE_PORT)
				.describedAs("Lumongo rest port");
		OptionSpec<String> uniqueIdArg = parser.accepts("uniqueId").withRequiredArg().required().describedAs("Unique Id");
		OptionSpec<String> fileNameArg = parser.accepts("fileName").withRequiredArg().required().describedAs("Associated File Name");
		OptionSpec<File> outputFileArg = parser.accepts("outputFile").withRequiredArg().ofType(File.class).required()
				.describedAs("Location to write associated file");
		
		try {
			OptionSet options = parser.parse(args);
			
			String address = options.valueOf(addressArg);
			int restPort = options.valueOf(restPortArg);
			String uniqueId = options.valueOf(uniqueIdArg);
			String fileName = options.valueOf(fileNameArg);
			File outputFile = options.valueOf(outputFileArg);
			
			LumongoRestClient client = new LumongoRestClient(address, restPort);
			client.fetchAssociated(uniqueId, fileName, outputFile);
			
		}
		catch (OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.formatHelpWith(new LumongoHelpFormatter());
			parser.printHelpOn(System.out);
		}
	}
}
