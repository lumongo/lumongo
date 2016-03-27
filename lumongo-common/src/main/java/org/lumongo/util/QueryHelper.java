package org.lumongo.util;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mdavis on 3/27/16.
 */
public class QueryHelper {

	public final static Function<String, String> ADD_QUOTES = (s) -> '"' + s + '"';

	public static String getOrQuery(String field, Collection<String> values, Function<String, String> modifier) {
		return field + ":" + values.stream().map(modifier).collect(Collectors.joining(" OR "));
	}

	public static String getOrTerms(Collection<String> values, Function<String, String> modifier) {
		return values.stream().map(modifier).collect(Collectors.joining(" OR "));
	}
}
