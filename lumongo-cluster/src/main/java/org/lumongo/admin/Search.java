package org.lumongo.admin;

import java.util.List;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.LumongoClient;
import org.lumongo.client.config.LumongoClientConfig;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.util.LogUtil;

public class Search {
	
	public static void main(String[] args) throws Exception {
		
		LogUtil.loadLogConfig();
		
		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts("address").withRequiredArg().defaultsTo("localhost").describedAs("Lumongo server address");
		OptionSpec<Integer> portArg = parser.accepts("port").withRequiredArg().ofType(Integer.class).defaultsTo(LumongoConstants.DEFAULT_EXTERNAL_SERVICE_PORT)
				.describedAs("Lumongo external port");
		OptionSpec<String> indexesArg = parser.accepts("index").withRequiredArg().required().describedAs("Index to search");
		OptionSpec<String> queryArg = parser.accepts("query").withRequiredArg().required().describedAs("Lucene query");
		OptionSpec<Integer> amountArg = parser.accepts("amount").withRequiredArg().required().ofType(Integer.class).describedAs("Amount of results to return");
		OptionSpec<Boolean> realTimeArg = parser.accepts("realTime").withRequiredArg().ofType(Boolean.class).defaultsTo(true).describedAs("Real time search");
		
		try {
			OptionSet options = parser.parse(args);
			
			List<String> indexes = options.valuesOf(indexesArg);
			String address = options.valueOf(addressArg);
			int port = options.valueOf(portArg);
			String query = options.valueOf(queryArg);
			int amount = options.valueOf(amountArg);
			boolean realTime = options.valueOf(realTimeArg);
			
			LumongoClientConfig lumongoClientConfig = new LumongoClientConfig();
			lumongoClientConfig.addMember(address, port);
			LumongoClient client = new LumongoClient(lumongoClientConfig);
			
			try {
				//force connect
				client.openConnection();
				
				long startTime = System.currentTimeMillis();
				
				QueryResponse qr = client.query(query, amount, indexes.toArray(new String[0]), realTime);
				List<ScoredResult> srList = qr.getResultsList();
				
				long endTime = System.currentTimeMillis();
				
				System.out.println("QueryTime: " + (endTime - startTime) + "ms");
				System.out.println("TotalResults: " + qr.getTotalHits());
				
				System.out.print("UniqueId");
				System.out.print("\t");
				System.out.print("Score");
				System.out.print("\t");
				System.out.print("Index");
				System.out.print("\t");
				System.out.print("Segment");
				System.out.print("\t");
				System.out.print("SegmentId");
				System.out.println();
				
				for (ScoredResult sr : srList) {
					System.out.print(sr.getUniqueId());
					System.out.print("\t");
					System.out.print(sr.getScore());
					System.out.print("\t");
					System.out.print(sr.getIndexName());
					System.out.print("\t");
					System.out.print(sr.getSegment());
					System.out.print("\t");
					System.out.print(sr.getDocId());
					System.out.println();
				}
			}
			finally {
				client.close();
			}
		}
		catch (OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.formatHelpWith(new LumongoHelpFormatter());
			parser.printHelpOn(System.out);
		}
	}
}
