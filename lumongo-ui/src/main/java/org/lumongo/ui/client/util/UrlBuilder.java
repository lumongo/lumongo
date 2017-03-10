package org.lumongo.ui.client.util;

import com.google.gwt.http.client.URL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Payam Meyer on 10/29/15.
 * @author pmeyer
 */
public class UrlBuilder {

	//Copied from com.google.gwt.user.client.Window.Location buildListParamMap
	public static Map<String, List<String>> buildListParamMap(String queryString) {

		if (queryString != null) {
			//places don't start with ? and the substring below removes the first char
			if (!queryString.startsWith("?")) {
				queryString = "?" + queryString;
			}

			Map<String, List<String>> out = new HashMap<String, List<String>>();

			if (queryString.length() > 1) {
				String qs = queryString.substring(1);

				for (String kvPair : qs.split("&")) {
					String[] kv = kvPair.split("=", 2);
					if (kv[0].length() == 0) {
						continue;
					}

					List<String> values = out.get(kv[0]);
					if (values == null) {
						values = new ArrayList<String>();
						out.put(kv[0], values);
					}
					values.add(kv.length > 1 ? URL.decodeQueryString(kv[1]) : "");
				}
			}

			for (Map.Entry<String, List<String>> entry : out.entrySet()) {
				entry.setValue(Collections.unmodifiableList(entry.getValue()));
			}

			out = Collections.unmodifiableMap(out);

			return out;
		}
		return Collections.emptyMap();
	}

	public static String getSingleValue(Map<String, List<String>> params, String key) {
		String value = null;
		if (params.containsKey(key)) {
			List<String> values = params.get(key);
			if (values.size() == 1) {
				value = values.get(0);
				if (value == null || value.isEmpty()) {
					value = null;
				}
			}
		}
		return value;
	}

}
