package org.lumongo.ui.client.util;

import com.google.gwt.http.client.URL;

import java.util.HashMap;
import java.util.Map;

/**
 * from com.google.gwt.http.client.UrlBuilder
 *
 * Created by pmeyer on 10/29/15.
 */
public class UrlParamBuilder {

	private Map<String, String[]> listParamMap = new HashMap<String, String[]>();

	/**
	 * Build the URL and return it as an encoded string.
	 *
	 * @return the encoded URL string
	 */
	public String buildString() {
		StringBuilder url = new StringBuilder();

		// Generate the query string.
		// http://www.google.com:80/path/to/file.html?k0=v0&k1=v1
		String prefix = "";
		for (Map.Entry<String, String[]> entry : listParamMap.entrySet()) {
			for (String val : entry.getValue()) {
				url.append(prefix).append(URL.encodeQueryString(entry.getKey())).append('=');
				if (val != null) {
					// Also encodes +,& etc.
					url.append(URL.encodeQueryString(val));
				}
				prefix = "&";
			}
		}

		return url.toString();
	}

	/**
	 * Remove a query parameter from the map.
	 *
	 * @param name the parameter name
	 */
	public UrlParamBuilder removeParameter(String name) {
		listParamMap.remove(name);
		return this;
	}

	/**
	 * <p>
	 * Set a query parameter to a list of values. Each value in the list will be
	 * added as its own key/value pair.
	 *
	 * <p>
	 * <h3>Example Output</h3>
	 * <code>?mykey=value0&mykey=value1&mykey=value2</code>
	 * </p>
	 *
	 * @param key the key
	 * @param values the list of values
	 */
	public UrlParamBuilder setParameter(String key, String... values) {
		assertNotNullOrEmpty(key, "Key cannot be null or empty", false);
		assertNotNull(values, "Values cannot null. Try using removeParameter instead.");
		if (values.length == 0) {
			throw new IllegalArgumentException("Values cannot be empty.  Try using removeParameter instead.");
		}
		listParamMap.put(key, values);
		return this;
	}

	/**
	 * Assert that the value is not null.
	 *
	 * @param value the value
	 * @param message the message to include with any exceptions
	 * @throws IllegalArgumentException if value is null
	 */
	private void assertNotNull(Object value, String message) throws IllegalArgumentException {
		if (value == null) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * Assert that the value is not null or empty.
	 *
	 * @param value the value
	 * @param message the message to include with any exceptions
	 * @param isState if true, throw a state exception instead
	 * @throws IllegalArgumentException if value is null
	 * @throws IllegalStateException if value is null and isState is true
	 */
	private void assertNotNullOrEmpty(String value, String message, boolean isState) throws IllegalArgumentException {
		if (value == null || value.length() == 0) {
			if (isState) {
				throw new IllegalStateException(message);
			}
			else {
				throw new IllegalArgumentException(message);
			}
		}
	}

}
