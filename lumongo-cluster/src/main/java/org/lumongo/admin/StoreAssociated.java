package org.lumongo.admin;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.LumongoRestClient;
import org.lumongo.util.LogUtil;

import javax.ws.rs.WebApplicationException;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;

public class StoreAssociated {

	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();

		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts(AdminConstants.ADDRESS).withRequiredArg().defaultsTo("localhost").describedAs("Lumongo server address");
		OptionSpec<Integer> restPortArg = parser.accepts(AdminConstants.REST_PORT).withRequiredArg().ofType(Integer.class)
						.defaultsTo(LumongoConstants.DEFAULT_REST_SERVICE_PORT).describedAs("Lumongo rest port");
		OptionSpec<String> idArg = parser.accepts(AdminConstants.ID).withRequiredArg().required().describedAs("Doc Id");
		OptionSpec<String> indexArg = parser.accepts(AdminConstants.INDEX).withRequiredArg().required().describedAs("Index");
		OptionSpec<String> fileNameArg = parser.accepts(AdminConstants.FILE_NAME).withRequiredArg().required().describedAs("Associated File Name");
		OptionSpec<File> fileToStoreArg = parser.accepts(AdminConstants.FILE_TO_STORE).withRequiredArg().ofType(File.class).required()
						.describedAs("Associated File to Store");
		OptionSpec<String> metaArg = parser.accepts(AdminConstants.META, "Meta data in form key=value").withRequiredArg();

		OptionSpec<Boolean> compressedArg = parser.accepts(AdminConstants.COMPRESSED).withRequiredArg().ofType(Boolean.class)
				.describedAs("Compress before storage");

		try {
			OptionSet options = parser.parse(args);

			String address = options.valueOf(addressArg);
			int restPort = options.valueOf(restPortArg);
			String id = options.valueOf(idArg);
			String indexName = options.valueOf(indexArg);
			String fileName = options.valueOf(fileNameArg);
			File fileToStore = options.valueOf(fileToStoreArg);
			Boolean compressed = options.valueOf(compressedArg);
			List<String> meta = options.valuesOf(metaArg);
			HashMap<String, String> metaMap = new HashMap<>();
			for (String m : meta) {
				int colonIndex = m.indexOf(":");
				if (colonIndex != -1) {
					String key = m.substring(0, colonIndex);
					String value = m.substring(colonIndex + 1);
					metaMap.put(key,value);
				}
				else {
					System.err.println("ERROR: Meta must be in the form key:value");
					System.exit(1);
				}
			}


			LumongoRestClient client = new LumongoRestClient(address, restPort);
			client.storeAssociated(id, indexName, fileName, metaMap, new FileInputStream(fileToStore), compressed);

		}
		catch (OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.formatHelpWith(new LumongoHelpFormatter());
			parser.printHelpOn(System.out);
			System.exit(2);
		}
	}
}
