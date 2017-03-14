package org.lumongo.ui.client.places;

import com.google.gwt.place.shared.Place;
import com.google.gwt.place.shared.PlaceTokenizer;
import org.lumongo.ui.client.util.UrlBuilder;
import org.lumongo.ui.client.util.UrlParamBuilder;

import java.util.List;
import java.util.Map;

/**
 * Created by Matt Davis on 10/16/16.
 * @author mdavis
 */
public class QueryPlace extends Place {

	private String indexName;
	private String queryId;

	public QueryPlace(String indexName, String queryId) {
		this.indexName = indexName;
		this.queryId = queryId;
	}

	public String getIndexName() {
		return indexName;
	}

	public String getQueryId() {
		return queryId;
	}

	@Override
	public String toString() {
		return "QueryPlace{" + "indexName='" + indexName + '\'' + ", queryId='" + queryId + '\'' + '}';
	}

	public static class Tokenizer implements PlaceTokenizer<QueryPlace> {

		@Override
		public QueryPlace getPlace(String token) {

			Map<String, List<String>> params = UrlBuilder.buildListParamMap(token);
			String indexName = UrlBuilder.getSingleValue(params, "indexName");
			String queryId = UrlBuilder.getSingleValue(params, "queryId");

			return new QueryPlace(indexName, queryId);
		}

		@Override
		public String getToken(QueryPlace place) {
			UrlParamBuilder builder = new UrlParamBuilder();
			if (place.getIndexName() != null) {
				builder.setParameter("indexName", place.getIndexName());
			}
			if (place.getQueryId() != null) {
				builder.setParameter("queryId", place.getQueryId());
			}

			return builder.buildString();
		}

	}

}