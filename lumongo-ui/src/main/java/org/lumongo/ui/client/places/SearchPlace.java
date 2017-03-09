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
public class SearchPlace extends Place {

	private String searchId;

	public SearchPlace(String searchId) {
		this.searchId = searchId;
	}

	public String getSearchId() {
		return searchId;
	}

	@Override
	public String toString() {
		return "SearchPlace{" + "searchId='" + searchId + '\'' + '}';
	}

	public static class Tokenizer implements PlaceTokenizer<SearchPlace> {

		@Override
		public SearchPlace getPlace(String token) {

			Map<String, List<String>> params = UrlBuilder.buildListParamMap(token);
			String searchId = UrlBuilder.getSingleValue(params, "searchId");

			return new SearchPlace(searchId);
		}

		@Override
		public String getToken(SearchPlace place) {
			UrlParamBuilder builder = new UrlParamBuilder();
			if (place.getSearchId() != null) {
				builder.setParameter("searchId", place.getSearchId());
			}

			return builder.buildString();
		}

	}

}