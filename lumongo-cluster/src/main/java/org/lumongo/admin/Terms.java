package org.lumongo.admin;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.LumongoClient;
import org.lumongo.client.config.LumongoClientConfig;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;
import org.lumongo.util.LogUtil;

public class Terms {
	private static final String ADDRESS = "address";
	private static final String PORT = "port";
	private static final String INDEX = "index";
	private static final String FIELD = "field";
	private static final String MIN_DOC_FREQ = "minDocFreq";
	private static final String START_TERM = "startTerm";
	
	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();
		
		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts(ADDRESS).withRequiredArg().defaultsTo("localhost").describedAs("Lumongo server address");
		OptionSpec<Integer> portArg = parser.accepts(PORT).withRequiredArg().ofType(Integer.class).defaultsTo(32191).describedAs("Lumongo external port");
		OptionSpec<String> indexArg = parser.accepts(INDEX).withRequiredArg().required().describedAs("Index to find terms");
		OptionSpec<String> fieldArg = parser.accepts(FIELD).withRequiredArg().required().describedAs("Field to find terms");
		OptionSpec<Integer> minDocFreqArg = parser.accepts(MIN_DOC_FREQ).withRequiredArg().ofType(Integer.class)
				.describedAs("Minimum number of documents for a term to exist");
		OptionSpec<String> startTermArg = parser.accepts(START_TERM).withRequiredArg().describedAs("Term to start search from");
		
		LumongoClient client = null;
		
		try {
			OptionSet options = parser.parse(args);
			
			String index = options.valueOf(indexArg);
			String address = options.valueOf(addressArg);
			int port = options.valueOf(portArg);
			String field = options.valueOf(fieldArg);
			Integer minDocFreq = options.valueOf(minDocFreqArg);
			String startTerm = options.valueOf(startTermArg);
			
			LumongoClientConfig lumongoClientConfig = new LumongoClientConfig();
			lumongoClientConfig.addMember(address, port);
			client = new LumongoClient(lumongoClientConfig);
			
			GetTermsResponse response = client.getTerms(index, field, startTerm, minDocFreq);
			
			System.out.print("Term");
			System.out.print("\t");
			System.out.println("DocFreq");
			for (Lumongo.Term term : response.getTermList()) {
				System.out.print(term.getValue());
				System.out.print("\t");
				System.out.println(term.getDocFreq());
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
