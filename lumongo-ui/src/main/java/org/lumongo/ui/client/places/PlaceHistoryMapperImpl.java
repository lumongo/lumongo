package org.lumongo.ui.client.places;

import com.google.gwt.place.shared.Place;
import com.google.gwt.place.shared.PlaceHistoryMapper;
import com.google.gwt.place.shared.PlaceTokenizer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Payam Meyer on 6/29/16.
 * @author pmeyer
 */
public class PlaceHistoryMapperImpl implements PlaceHistoryMapper {

	private static final List<PlacePojo> DEFAULT_PLACES = Arrays.asList(new PlacePojo<>("home", new HomePlace.Tokenizer(), HomePlace.class),
			new PlacePojo<>("search", new SearchPlace.Tokenizer(), SearchPlace.class));

	private Map<String, PlaceTokenizer<Place>> tokenizersByPrefix = new HashMap<>();
	private Map<Class<?>, PlaceTokenizer<Place>> tokenizersByPlace = new HashMap<>();
	private Map<Class<?>, String> prefixesByPlace = new HashMap<>();

	public PlaceHistoryMapperImpl() {
		for (PlacePojo customPlace : DEFAULT_PLACES) {
			addPlace(customPlace);
		}
	}

	public PlaceHistoryMapperImpl addPlace(PlacePojo placePojo) {
		String prefix = placePojo.getPrefix();
		PlaceTokenizer<Place> tokenizer = (PlaceTokenizer<Place>) placePojo.getTokenizer();
		Class<?> placeClass = (Class<?>) placePojo.getPlaceClass();
		tokenizersByPrefix.put(prefix, tokenizer);
		tokenizersByPlace.put(placeClass, tokenizer);
		prefixesByPlace.put(placeClass, prefix);
		return this;
	}

	@Override
	public Place getPlace(String token) {
		int separatorAt = token.indexOf(':');
		String prefix;
		String rest;
		if (separatorAt >= 0) {
			prefix = token.substring(0, separatorAt);
			rest = token.substring(separatorAt + 1);
		}
		else {
			prefix = token;
			rest = null;
		}
		PlaceTokenizer<?> tokenizer = tokenizersByPrefix.get(prefix);
		if (tokenizer != null) {
			return tokenizer.getPlace(rest);
		}
		return null;
	}

	@Override
	public String getToken(Place place) {
		PlaceTokenizer<Place> placeTokenizer = tokenizersByPlace.get(place.getClass());
		String token = prefixesByPlace.get(place.getClass());
		String rest = placeTokenizer.getToken(place);
		if (rest != null && rest.trim().length() > 0)
			token += ":" + rest;
		return token;
	}
}
