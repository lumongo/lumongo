package org.lumongo.ui.server;

import com.cedarsoftware.util.io.JsonWriter;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.util.JSONSerializers;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.lumongo.client.command.GetFields;
import org.lumongo.client.command.Query;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.ui.client.services.UIQueryService;
import org.lumongo.ui.shared.IndexInfo;
import org.lumongo.ui.shared.InstanceInfo;
import org.lumongo.ui.shared.UIQueryObject;
import org.lumongo.ui.shared.UIQueryResults;
import org.lumongo.util.ResultHelper;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Payam Meyer on 3/9/17.
 * @author pmeyer
 */
public class UIQueryServiceImpl extends RemoteServiceServlet implements UIQueryService {

	private static final Logger LOG = LoggerFactory.getLogger(UIQueryServiceImpl.class);
	private static final int MB = 1024 * 1024;
	private static final String QUERY_HISTORY = "queryHistory";
	private LumongoWorkPool lumongoWorkPool;
	private String lumongoVersion;
	private String luceneVersion;
	private Datastore datastore;

	@Override
	public void init() throws ServletException {
		super.init();

		try {
			LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig().addMember("localhost").setDefaultRetries(2);
			lumongoPoolConfig.setMemberUpdateEnabled(false);
			lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);

			Properties properties = new Properties();
			properties.load(UIQueryServiceImpl.class.getResourceAsStream("/version.properties"));
			lumongoVersion = properties.getProperty("lumongoVersion");
			luceneVersion = properties.getProperty("luceneVersion");

			MongoClientOptions mongoClientOptions = MongoClientOptions.builder().connectionsPerHost(32).build();
			MongoClient mongoClient = new MongoClient("localhost", mongoClientOptions);
			Morphia morphia = new Morphia();
			morphia.map(UIQueryObject.class);
			datastore = morphia.createDatastore(mongoClient, QUERY_HISTORY);
			datastore.ensureIndexes();
		}
		catch (Exception e) {
			LOG.error("Failed to initiate lumongo work pool.", e);
		}

	}

	@Override
	public InstanceInfo getInstanceInfo() throws Exception {
		InstanceInfo instanceInfo = new InstanceInfo();
		instanceInfo.setLumongoVersion(lumongoVersion);
		instanceInfo.setLuceneVersion(luceneVersion);

		List<IndexInfo> indexInfoList = getIndexInfos();

		instanceInfo.setIndexes(indexInfoList);

		Runtime runtime = Runtime.getRuntime();

		// TODO: These need to be LuMongo's not this app, who cares about this app?
		instanceInfo.setJvmUsedMemory((runtime.totalMemory() - runtime.freeMemory()) / MB);
		instanceInfo.setJvmFreeMemory(runtime.freeMemory() / MB);
		instanceInfo.setJvmTotalMemoryMB(runtime.totalMemory() / MB);
		instanceInfo.setJvmMaxMemoryMB(runtime.maxMemory() / MB);

		return instanceInfo;
	}

	private List<IndexInfo> getIndexInfos() throws Exception {
		GetIndexesResult indexes = lumongoWorkPool.getIndexes();

		List<IndexInfo> indexInfoList = new ArrayList<>();
		for (String indexName : indexes.getIndexNames()) {
			IndexInfo indexInfo = new IndexInfo();
			indexInfo.setName(indexName);
			indexInfo.setSize(20L);
			indexInfo.setTotalDocs((int) lumongoWorkPool.getNumberOfDocs(indexName).getNumberOfDocs());
			indexInfo.setFieldNames(new ArrayList<>(lumongoWorkPool.getFields(new GetFields(indexName)).getFieldNames()));

			indexInfoList.add(indexInfo);
		}
		return indexInfoList;
	}

	@Override
	public UIQueryResults search(String queryId) throws Exception {

		try {
			UIQueryResults results = new UIQueryResults();

			UIQueryObject uiQueryObject = datastore.createQuery(UIQueryObject.class).field("_id").equal(new ObjectId(queryId)).get();
			results.setUiQueryObject(uiQueryObject);
			results.setIndexes(getIndexInfos());

			Query query = new Query(uiQueryObject.getIndexNames(), uiQueryObject.getQuery(), uiQueryObject.getRows());
			if (query.getQuery().isEmpty()) {
				query.setQuery("*:*");
			}

			query.setDebug(uiQueryObject.isDebug());

			if (uiQueryObject.getStart() != 0) {
				query.setStart(uiQueryObject.getStart());
			}

			if (uiQueryObject.isDontCache()) {
				query.setDontCache(uiQueryObject.isDontCache());
			}

			if (uiQueryObject.getMm() != null) {
				query.setMinimumNumberShouldMatch(uiQueryObject.getMm());
			}
			if (uiQueryObject.isDismax() != null) {
				query.setDismax(uiQueryObject.isDismax());
				if (uiQueryObject.getDismaxTie() != null) {
					query.setDismaxTie(uiQueryObject.getDismaxTie());
				}
			}
			if (!uiQueryObject.getQueryFields().isEmpty()) {
				uiQueryObject.getQueryFields().forEach(query::addQueryField);
			}
			if (uiQueryObject.getDefaultOperator() != null) {
				String defaultOperator = uiQueryObject.getDefaultOperator();
				if (defaultOperator.equalsIgnoreCase("AND")) {
					query.setDefaultOperator(Lumongo.Query.Operator.AND);
				}
				else if (defaultOperator.equalsIgnoreCase("OR")) {
					query.setDefaultOperator(Lumongo.Query.Operator.OR);
				}
			}

			if (uiQueryObject.getSimilarities() != null) {
				for (String field : uiQueryObject.getSimilarities().keySet()) {
					String simType = uiQueryObject.getSimilarities().get(field);

					if (simType.equalsIgnoreCase("bm25")) {
						query.addFieldSimilarity(field, Lumongo.AnalyzerSettings.Similarity.BM25);
					}
					else if (simType.equalsIgnoreCase("constant")) {
						query.addFieldSimilarity(field, Lumongo.AnalyzerSettings.Similarity.CONSTANT);
					}
					else if (simType.equalsIgnoreCase("tf")) {
						query.addFieldSimilarity(field, Lumongo.AnalyzerSettings.Similarity.TF);
					}
					else if (simType.equalsIgnoreCase("tfidf")) {
						query.addFieldSimilarity(field, Lumongo.AnalyzerSettings.Similarity.TFIDF);
					}

				}
			}

			if (uiQueryObject.getFilterQueries() != null) {
				for (String filterQuery : uiQueryObject.getFilterQueries()) {
					query.addFilterQuery(filterQuery);
				}
			}

			// TODO: This needs to pass in a proper object
			if (uiQueryObject.getCosineSimJsonList() != null) {

			}

			// TODO: ditto
			if (uiQueryObject.getFilterJsonQueries() != null) {

			}

			// TODO: This needs to pass in an object with other parameters and not just the field name.
			if (uiQueryObject.getHighlightList() != null) {
				for (String field : uiQueryObject.getHighlightList()) {
					query.addHighlight(field);
				}
			}

			// TODO: ditto to the ditto
			if (uiQueryObject.getHighlightJsonList() != null) {

			}

			// TODO: ditto^3
			if (uiQueryObject.getAnalyzeJsonList() != null) {

			}

			if (uiQueryObject.getDisplayFields() != null) {
				for (String field : uiQueryObject.getDisplayFields()) {
					if (field.startsWith("-")) {
						query.addDocumentMaskedField(field.substring(1, field.length()));
					}
					else {
						query.addDocumentField(field);
					}
				}
			}

			query.setResultFetchType(Lumongo.FetchType.FULL);

			// TODO: do drill down?
			if (uiQueryObject.getDrillDowns() != null) {

			}

			for (String sortField : uiQueryObject.getSortList().keySet()) {
				String sortDir = uiQueryObject.getSortList().get(sortField);

				if ("-1".equals(sortDir) || "DESC".equalsIgnoreCase(sortDir)) {
					query.addFieldSort(sortField, Lumongo.FieldSort.Direction.DESCENDING);
				}
				else if ("1".equals(sortDir) || "ASC".equalsIgnoreCase(sortDir)) {
					query.addFieldSort(sortField, Lumongo.FieldSort.Direction.ASCENDING);
				}
			}

			LOG.info("Query: " + query);
			QueryResult queryResult = lumongoWorkPool.query(query);

			results.setTotalResults(queryResult.getTotalHits());

			for (Lumongo.ScoredResult scoredResult : queryResult.getResults()) {
				Document document = ResultHelper.getDocumentFromScoredResult(scoredResult);
				if (document != null) {
					// always add index name and ID
					document.put("indexName", scoredResult.getIndexName());
					document.put("id", scoredResult.getUniqueId());
					String jsonDoc = JSONSerializers.getLegacy().serialize(document);
					results.addFormattedDocument(JsonWriter.formatJson(jsonDoc));
				}
			}

			for (String facet : uiQueryObject.getFacets()) {
				for (Lumongo.FacetCount fc : queryResult.getFacetCounts(facet)) {
					results.addFacetCount(fc.getFacet(), fc.getCount());
				}
			}

			return results;
		}
		catch (Exception e) {
			LOG.error("Failed to execute query.", e);
			throw e;
		}
	}

	@Override
	public String saveQuery(UIQueryObject uiQueryObject) throws Exception {
		try {
			return datastore.save(uiQueryObject).getId().toString();
		}
		catch (Exception e) {
			LOG.error("Failed to save the query.");
			throw e;
		}
	}

}
