package org.lumongo.admin;

import com.google.protobuf.ServiceException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.bson.BSON;
import org.lumongo.LumongoConstants;
import org.lumongo.admin.help.LumongoHelpFormatter;
import org.lumongo.client.command.Query;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoBaseWorkPool;
import org.lumongo.client.pool.LumongoPool;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FacetGroup;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.util.LogUtil;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;

public class Search {

	public static void main(String[] args) throws Exception {

		DecimalFormat df = new DecimalFormat("#.00");

		LogUtil.loadLogConfig();

		OptionParser parser = new OptionParser();
		OptionSpec<String> addressArg = parser.accepts(AdminConstants.ADDRESS, "LuMongo server address").withRequiredArg().defaultsTo("localhost");
		OptionSpec<Integer> portArg = parser.accepts(AdminConstants.PORT, "LuMongo external port").withRequiredArg().ofType(Integer.class)
				.defaultsTo(LumongoConstants.DEFAULT_EXTERNAL_SERVICE_PORT);
		OptionSpec<String> indexesArg = parser.accepts(AdminConstants.INDEX, "Index to search").withRequiredArg().required();
		OptionSpec<String> queryArg = parser.accepts(AdminConstants.QUERY, "Lucene query (matches all docs by default)").withRequiredArg();
		OptionSpec<Integer> amountArg = parser.accepts(AdminConstants.AMOUNT, "Amount of results to return").withRequiredArg().ofType(Integer.class)
				.defaultsTo(10);

		OptionSpec<Integer> startArg = parser.accepts(AdminConstants.START, "Start index").withRequiredArg().ofType(Integer.class)
				.defaultsTo(0);
		OptionSpec<String> facetsArg = parser.accepts(AdminConstants.FACET, "Count facets on").withRequiredArg();
		OptionSpec<Integer> facetsCountArg = parser.accepts(AdminConstants.FACET_COUNT, "Number of facets to return").withRequiredArg().ofType(Integer.class)
				.defaultsTo(10);
		OptionSpec<Integer> facetSegmentCountArg = parser.accepts(AdminConstants.FACET_SEGMENT_COUNT, "Number of facets to return per segment")
				.withRequiredArg().ofType(Integer.class).defaultsTo(40);
		OptionSpec<String> sortArg = parser.accepts(AdminConstants.SORT, "Field to sort on").withRequiredArg();
		OptionSpec<String> sortDescArg = parser.accepts(AdminConstants.SORT_DESC, "Field to sort on (descending)").withRequiredArg();
		OptionSpec<String> queryFieldArg = parser
				.accepts(AdminConstants.QUERY_FIELD, "Specific field(s) for query to search if none specified in query instead of index default")
				.withRequiredArg();
		OptionSpec<String> filterQueryArg = parser.accepts(AdminConstants.FILTER_QUERY, "Filter query").withRequiredArg();
		OptionSpec<Integer> minimumNumberShouldMatchArg = parser.accepts(AdminConstants.MIN_TO_MATCH, "Minimum number of optional boolean queries to match")
				.withRequiredArg().ofType(Integer.class);

		OptionSpec<Void> fetchArg = parser.accepts(AdminConstants.FETCH);

		OptionSpec<String> fieldsToReturnArg = parser.accepts(AdminConstants.RETURN, "Fields to return from fetch").withRequiredArg();
		OptionSpec<String> fieldsToMaskArg = parser.accepts(AdminConstants.MASK, "Fields to mask from fetch").withRequiredArg();

		int exitCode = 0;
		LumongoWorkPool lumongoWorkPool = null;
		try {
			OptionSet options = parser.parse(args);

			List<String> indexes = options.valuesOf(indexesArg);
			String address = options.valueOf(addressArg);
			int port = options.valueOf(portArg);
			String query = options.valueOf(queryArg);
			int amount = options.valueOf(amountArg);
			int start = options.valueOf(startArg);
			List<String> facets = options.valuesOf(facetsArg);
			Integer facetCount = options.valueOf(facetsCountArg);
			Integer facetSegmentCount = options.valueOf(facetSegmentCountArg);

			List<String> sortList = options.valuesOf(sortArg);
			List<String> sortDescList = options.valuesOf(sortDescArg);
			List<String> queryFieldsList = options.valuesOf(queryFieldArg);
			List<String> filterQueryList = options.valuesOf(filterQueryArg);
			Integer minimumNumberShouldMatch = options.valueOf(minimumNumberShouldMatchArg);

			List<String> fieldsToReturn = options.valuesOf(fieldsToReturnArg);
			List<String> fieldsToMask = options.valuesOf(fieldsToMaskArg);

			boolean fetch = options.has(fetchArg);

			LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
			lumongoPoolConfig.addMember(address, port);
			lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);

			Query q = new Query(indexes, query, amount);

			q.setStart(start);

			if (fetch) {
				q.setResultFetchType(Lumongo.FetchType.FULL);
			}

			if (minimumNumberShouldMatch != null) {
				q.setMinimumNumberShouldMatch(minimumNumberShouldMatch);
			}

			for (String facet : facets) {
				q.addCountRequest(facet, facetCount, facetSegmentCount);
			}

			sortList.forEach(q::addFieldSort);

			for (String sortDesc : sortDescList) {
				q.addFieldSort(sortDesc, Direction.DESCENDING);
			}

			queryFieldsList.forEach(q::addQueryField);

			filterQueryList.forEach(q::addFilterQuery);

			fieldsToReturn.forEach(q::addDocumentField);
			fieldsToMask.forEach(q::addDocumentMaskedField);

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
			System.out.print("\t");
			if (fetch) {
				System.out.print("Document");
			}
			System.out.println();

			for (ScoredResult sr : srList) {
				System.out.print(sr.getUniqueId());
				System.out.print("\t");
				System.out.print(df.format(sr.getScore()));
				System.out.print("\t");
				System.out.print(sr.getIndexName());
				System.out.print("\t");
				System.out.print(sr.getSegment());
				System.out.print("\t");
				System.out.print(sr.getDocId());
				System.out.print("\t");

				StringBuffer sb = new StringBuffer();

				if (sr.hasSortValues()) {
					for (Lumongo.SortValue sortValue : sr.getSortValues().getSortValueList()) {
						if (sb.length() != 0) {
							sb.append(",");
						}
						if (sortValue.getExists()) {
							if (sortValue.hasDateValue()) {
								sb.append(new Date(sortValue.getDateValue()));
							}
							else if (sortValue.hasDoubleValue()) {
								sb.append(sortValue.getDoubleValue());
							}
							else if (sortValue.hasFloatValue()) {
								sb.append(sortValue.getFloatValue());
							}
							else if (sortValue.hasIntegerValue()) {
								sb.append(sortValue.getIntegerValue());
							}
							else if (sortValue.hasLongValue()) {
								sb.append(sortValue.getLongValue());
							}
							else if (sortValue.hasStringValue()) {
								sb.append(sortValue.getStringValue());
							}
						}
						else {
							sb.append("!NULL!");
						}
					}
				}

				if (sb.length() != 0) {
					System.out.print(sb);
				}
				else {
					System.out.print("--");
				}

				if (fetch) {
					System.out.print("\t");
					if (sr.hasResultDocument()) {
						Lumongo.ResultDocument resultDocument = sr.getResultDocument();
						if (resultDocument.hasDocument()) {
							DBObject document = new BasicDBObject();
							document.putAll(BSON.decode(resultDocument.getDocument().toByteArray()));
							System.out.println(document);
						}
					}
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
						System.out.print("\t");
						System.out.print("+" + fc.getMaxError());
						System.out.println();
					}
					if (fg.getPossibleMissing()) {
						System.out.println(
								"Possible facets missing from top results for <" + fg.getCountRequest().getFacetField().getLabel() + "> with max count <" + fg
										.getMaxValuePossibleMissing() + ">");
					}
				}

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
