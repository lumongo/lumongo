package org.lumongo.ui.client.places;

import com.google.gwt.place.shared.Place;
import com.google.gwt.place.shared.PlaceTokenizer;

/**
 * Created by Payam Meyer on 6/29/16.
 * @author pmeyer
 */
public class PlacePojo<T extends Place> {

	private String prefix;
	private PlaceTokenizer<? extends Place> tokenizer;
	private Class placeClass;

	public PlacePojo(String prefix, PlaceTokenizer<T> tokenizer, Class<T> placeClass) {
		this.prefix = prefix;
		this.tokenizer = tokenizer;
		this.placeClass = placeClass;
	}

	public String getPrefix() {
		return prefix;
	}

	public PlaceTokenizer<? extends Place> getTokenizer() {
		return tokenizer;
	}

	public Class getPlaceClass() {
		return placeClass;
	}
}
