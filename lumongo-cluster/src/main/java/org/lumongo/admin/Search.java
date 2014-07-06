package org.lumongo.admin;

import java.util.List;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.command.BatchFetch;
import org.lumongo.client.command.Query;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoBaseWorkPool;
import org.lumongo.client.pool.LumongoPool;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FacetGroup;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
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
		OptionSpec<String> facetsArg = parser.accepts(AdminConstants.FACET).withRequiredArg().describedAs("Count facets on");
		OptionSpec<String> drillDownArg = parser.accepts(AdminConstants.DRILL_DOWN).withRequiredArg().describedAs("Drill down on");
		OptionSpec<String> sortArg = parser.accepts(AdminConstants.SORT).withRequiredArg().describedAs("Field to sort on");
		OptionSpec<String> sortDescArg = parser.accepts(AdminConstants.SORT_DESC).withRequiredArg().describedAs("Field to sort on (descending)");
		OptionSpec<String> queryFieldArg = parser.accepts(AdminConstants.QUERY_FIELD).withRequiredArg()
						.describedAs("Specific field(s) for query to search if none specified in query instead of index default");
		OptionSpec<Void> fetchArg = parser.accepts(AdminConstants.FETCH);
		
		try {
			OptionSet options = parser.parse(args);
			
			List<String> indexes = options.valuesOf(indexesArg);
			String address = options.valueOf(addressArg);
			int port = options.valueOf(portArg);
			String query = options.valueOf(queryArg);
			int amount = options.valueOf(amountArg);
			Boolean realTime = options.valueOf(realTimeArg);
			List<String> facets = options.valuesOf(facetsArg);
			List<String> drillDowns = options.valuesOf(drillDownArg);
			List<String> sortList = options.valuesOf(sortArg);
			List<String> sortDescList = options.valuesOf(sortDescArg);
			List<String> queryFieldsList = options.valuesOf(queryFieldArg);
			boolean fetch = options.has(fetchArg);
			
			LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
			lumongoPoolConfig.addMember(address, port);
			LumongoBaseWorkPool lumongoWorkPool = new LumongoBaseWorkPool(new LumongoPool(lumongoPoolConfig));
			
			try {
				
				Query q = new Query(indexes, query, amount);
				
				for (String facet : facets) {
					q.addCountRequest(facet);
				}
				
				for (String drillDown : drillDowns) {
					q.addDrillDown(drillDown);
				}
				
				for (String sort : sortList) {
					
					q.addFieldSort(sort);
				}
				
				for (String sortDesc : sortDescList) {
					q.addFieldSort(sortDesc, Direction.DESCENDING);
				}
				
				for (String queryField : queryFieldsList) {
					q.addQueryField(queryField);
				}
				
				q.setRealTime(realTime);
				
				QueryResult qr = lumongoWorkPool.execute(q);
				
				List<ScoredResult> srList = qr.getResults();
				
				System.out.println("QueryTime: " + (qr.getCommandTimeMs()) + "ms");
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
				System.out.print("\t");
				System.out.print("Sort");
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
					System.out.print("\t");
					
					StringBuffer sb = new StringBuffer();
					
					for (String s : sr.getSortTermList()) {
						if (sb.length() != 0) {
							sb.append(",");
						}
						sb.append(s);
					}
					for (Integer i : sr.getSortIntegerList()) {
						if (sb.length() != 0) {
							sb.append(",");
						}
						sb.append(i);
					}
					for (Long l : sr.getSortLongList()) {
						if (sb.length() != 0) {
							sb.append(",");
						}
						sb.append(l);
					}
					for (Float f : sr.getSortFloatList()) {
						if (sb.length() != 0) {
							sb.append(",");
						}
						sb.append(f);
					}
					for (Double d : sr.getSortDoubleList()) {
						if (sb.length() != 0) {
							sb.append(",");
						}
						sb.append(d);
					}
					
					if (sb.length() != 0) {
						System.out.print(sb);
					}
					else {
						System.out.print("--");
					}
					
					System.out.println();
				}
				
				if (!qr.getFacetGroups().isEmpty()) {
					System.out.println("Facets:");
					for (FacetGroup fg : qr.getFacetGroups()) {
						System.out.println();
						System.out.println("--Facet on " + fg.getCountRequest().getFacetField().getLabel() + "--");
						for (FacetCount fc : fg.getFacetCountList()) {
							System.out.print(fc.getFacet());
							System.out.print("\t");
							System.out.print(fc.getCount());
							System.out.println();
						}
					}
					
				}
				
				if (fetch) {
					System.out.println("\nDocuments\n");
					BatchFetch batchFetch = new BatchFetch();
					batchFetch.addFetchDocumentsFromResults(srList);
					
					BatchFetchResult bfr = lumongoWorkPool.execute(batchFetch);
					
					for (FetchResult fetchResult : bfr.getFetchResults()) {
						System.out.println();
						
						if (fetchResult.hasResultDocument()) {
							
							if (fetchResult.isDocumentText()) {
								System.out.println(fetchResult.getUniqueId() + ":\n" + fetchResult.getDocumentAsUtf8());
							}
							else if (fetchResult.isDocumentBson()) {
								System.out.println(fetchResult.getUniqueId() + ":\n" + fetchResult.getDocumentAsBson());
							}
							else {
								System.out.println(fetchResult.getUniqueId() + ":\n [binary]");
							}
						}
						else {
							System.out.println(fetchResult.getUniqueId() + ":\n" + "Failed to fetch");
						}
					}
				}
			}
			finally {
				if (lumongoWorkPool != null) {
					lumongoWorkPool.shutdown();
				}
			}
		}
		catch (OptionException e) {
			System.err.println("ERROR: " + e.getMessage());
			parser.formatHelpWith(new LumongoHelpFormatter());
			parser.printHelpOn(System.out);
		}
	}
}
