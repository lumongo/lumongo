package org.lumongo.admin;

import com.google.protobuf.ServiceException;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.command.GetAllTerms;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoBaseWorkPool;
import org.lumongo.client.pool.LumongoPool;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.GetTermsResult;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.util.LogUtil;

import java.io.IOException;

public class Terms {
	
	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();
		
		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts(AdminConstants.ADDRESS).withRequiredArg().defaultsTo("localhost").describedAs("Lumongo server address");
		OptionSpec<Integer> portArg = parser.accepts(AdminConstants.PORT).withRequiredArg().ofType(Integer.class).defaultsTo(32191)
						.describedAs("Lumongo external port");
		OptionSpec<String> indexArg = parser.accepts(AdminConstants.INDEX).withRequiredArg().required().describedAs("Index to find terms");
		OptionSpec<String> fieldArg = parser.accepts(AdminConstants.FIELD).withRequiredArg().required().describedAs("Field to find terms");
		OptionSpec<Integer> minDocFreqArg = parser.accepts(AdminConstants.MIN_DOC_FREQ).withRequiredArg().ofType(Integer.class)
						.describedAs("Minimum number of documents for a term to exist");
		OptionSpec<String> startTermArg = parser.accepts(AdminConstants.START_TERM).withRequiredArg().describedAs("Term to start search from");
		OptionSpec<String> termFilterArg = parser.accepts(AdminConstants.TERM_FILTER).withRequiredArg().describedAs("Filter terms that match this regex");
		OptionSpec<String> termMatchArg = parser.accepts(AdminConstants.TERM_MATCH).withRequiredArg().describedAs("Return terms that match this regex");

		int exitCode = 0;
		LumongoWorkPool lumongoWorkPool = null;

		try {
			OptionSet options = parser.parse(args);
			
			String index = options.valueOf(indexArg);
			String address = options.valueOf(addressArg);
			int port = options.valueOf(portArg);
			String field = options.valueOf(fieldArg);
			Integer minDocFreq = options.valueOf(minDocFreqArg);
			String startTerm = options.valueOf(startTermArg);
			String termFilter = options.valueOf(termFilterArg);
			String termMatch = options.valueOf(termMatchArg);
			
			LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
			lumongoPoolConfig.addMember(address, port);
			lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);
			
			GetTermsResult response = lumongoWorkPool.execute(new GetAllTerms(index, field).setStartTerm(startTerm).setMinDocFreq(minDocFreq)
							.setTermFilter(termFilter).setTermMatch(termMatch));
			
			System.out.print("Term");
			System.out.print("\t");
			System.out.println("DocFreq");
			for (Lumongo.Term term : response.getTerms()) {
				System.out.print(term.getValue());
				System.out.print("\t");
				System.out.println(term.getDocFreq());
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
