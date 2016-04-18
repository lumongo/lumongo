package org.lumongo.admin;

import com.google.protobuf.ServiceException;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.command.GetTerms;
import org.lumongo.client.config.LumongoPoolConfig;
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
				.describedAs("Minimum number of documents for a term to be shown");
		OptionSpec<Integer> minTermFreqArg = parser.accepts(AdminConstants.MIN_TERM_FREQ).withRequiredArg().ofType(Integer.class)
				.describedAs("Minimum number of occurrences for a term to be shown");
		OptionSpec<String> startTermArg = parser.accepts(AdminConstants.START_TERM).withRequiredArg().describedAs("Term to start search from (inclusive)");
		OptionSpec<String> endTermArg = parser.accepts(AdminConstants.END_TERM).withRequiredArg().describedAs("Term to end on (exclusive)");
		OptionSpec<String> termFilterArg = parser.accepts(AdminConstants.TERM_FILTER).withRequiredArg().describedAs("Filter terms that match this regex");
		OptionSpec<String> termMatchArg = parser.accepts(AdminConstants.TERM_MATCH).withRequiredArg().describedAs("Return terms that match this regex");
		OptionSpec<Integer> amountArg = parser.accepts(AdminConstants.AMOUNT).withRequiredArg().describedAs("Number of terms to return (default 0/all terms)").ofType(Integer.class);

		int exitCode = 0;
		LumongoWorkPool lumongoWorkPool = null;

		try {
			OptionSet options = parser.parse(args);

			String index = options.valueOf(indexArg);
			String address = options.valueOf(addressArg);
			int port = options.valueOf(portArg);
			String field = options.valueOf(fieldArg);
			Integer minDocFreq = options.valueOf(minDocFreqArg);
			Integer minTermFreq = options.valueOf(minTermFreqArg);
			Integer amount = options.valueOf(amountArg);
			String startTerm = options.valueOf(startTermArg);
			String endTerm = options.valueOf(endTermArg);
			String termFilter = options.valueOf(termFilterArg);
			String termMatch = options.valueOf(termMatchArg);

			LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
			lumongoPoolConfig.addMember(address, port);
			lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);

			GetTermsResult response = lumongoWorkPool
					.execute(new GetTerms(index, field).setStartTerm(startTerm).setEndTerm(endTerm).setAmount(amount).setMinDocFreq(minDocFreq).setMinTermFreq(minTermFreq).setTermFilter(termFilter).setTermMatch(termMatch));

			System.out.print("Term");
			System.out.print("\t");
			System.out.print("DocFreq");
			System.out.print("\t");
			System.out.println("TermFreq");
			for (Lumongo.Term term : response.getTerms()) {
				System.out.print(term.getValue());
				System.out.print("\t");
				System.out.print(term.getDocFreq());
				System.out.print("\t");
				System.out.println(term.getTermFreq());
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
