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
import org.lumongo.cluster.message.Lumongo.CountRequest;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FacetRequest;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.util.LogUtil;

public class Search {
	
	public static void main(String[] args) throws Exception {
		
		LogUtil.loadLogConfig();
		
		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts(AdminConstants.ADDRESS).withRequiredArg().defaultsTo("localhost").describedAs("Lumongo server address");
		OptionSpec<Integer> portArg = parser.accepts(AdminConstants.PORT).withRequiredArg().ofType(Integer.class)
				.defaultsTo(LumongoConstants.DEFAULT_EXTERNAL_SERVICE_PORT).describedAs("Lumongo external port");
		OptionSpec<String> indexesArg = parser.accepts(AdminConstants.INDEX).withRequiredArg().required().describedAs("Index to search");
		OptionSpec<String> queryArg = parser.accepts(AdminConstants.QUERY).withRequiredArg().required().describedAs("Lucene query");
		OptionSpec<Integer> amountArg = parser.accepts(AdminConstants.AMOUNT).withRequiredArg().required().ofType(Integer.class)
				.describedAs("Amount of results to return");
		OptionSpec<Boolean> realTimeArg = parser.accepts(AdminConstants.REAL_TIME).withRequiredArg().ofType(Boolean.class).describedAs("Real time search");
		OptionSpec<String> facetsArg = parser.accepts(AdminConstants.FACET).withRequiredArg().required().describedAs("Facet on field");
		
		try {
			OptionSet options = parser.parse(args);
			
			List<String> indexes = options.valuesOf(indexesArg);
			String address = options.valueOf(addressArg);
			int port = options.valueOf(portArg);
			String query = options.valueOf(queryArg);
			int amount = options.valueOf(amountArg);
			Boolean realTime = options.valueOf(realTimeArg);
			List<String> facets = options.valuesOf(facetsArg);
			
			LumongoClientConfig lumongoClientConfig = new LumongoClientConfig();
			lumongoClientConfig.addMember(address, port);
			LumongoClient client = new LumongoClient(lumongoClientConfig);
			
			try {
				//force connect
				client.openConnection();
				
				long startTime = System.currentTimeMillis();
				
				FacetRequest.Builder fr = FacetRequest.newBuilder();
				for (String facet : facets) {
					fr.addCountRequest(CountRequest.newBuilder().setFacet(facet));
				}
				
				QueryResponse qr;
				if (fr.getCountRequestList().isEmpty()) {
					qr = client.query(query, amount, indexes.toArray(new String[0]), fr.build(), realTime);
				}
				else {
					//TODO fix so not required to do this
					qr = client.query(query, amount, indexes.toArray(new String[0]), realTime);
				}
				
				List<ScoredResult> srList = qr.getResultsList();
				
				long endTime = System.currentTimeMillis();
				
				System.out.println("QueryTime: " + (endTime - startTime) + "ms");
				System.out.println("TotalResults: " + qr.getTotalHits());
				
				System.out.println("Results:");
				
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
				
				if (!qr.getFacetCountList().isEmpty()) {
					System.out.println("Facets:");
					System.out.print("Facet");
					System.out.print("\t");
					System.out.print("Count");
					System.out.println();
					for (FacetCount fc : qr.getFacetCountList()) {
						System.out.print(fc.getFacet());
						System.out.print("\t");
						System.out.print(fc.getCount());
						System.out.println();
					}
					
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
