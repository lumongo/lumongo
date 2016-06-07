package org.lumongo.util;

import org.lumongo.LumongoConstants;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;

public class HttpHelper {
	public static String createQuery(HashMap<String, Object> parameters) {

		StringBuilder sb = new StringBuilder();

		for (String key : parameters.keySet()) {


			Object value = parameters.get(key);
			if (value instanceof String) {
				if (sb.length() > 0) {
					sb.append('&');
				}

				sb.append(key);
				sb.append('=');

				try {
					sb.append(URLEncoder.encode((String) value, LumongoConstants.UTF8));
				}
				catch (UnsupportedEncodingException e) {
					//should not be possible
					throw new RuntimeException(e);
				}

			}
			else if (value instanceof List) {
				List<String> stringList = (List<String>) value;
				for (String item : stringList) {

					if (sb.length() > 0) {
						sb.append('&');
					}

					sb.append(key);
					sb.append('=');
					try {
						sb.append(URLEncoder.encode(item, LumongoConstants.UTF8));
					}
					catch (UnsupportedEncodingException e) {
						//should not be possible
						throw new RuntimeException(e);
					}
				}
			}
		}
		return sb.toString();
	}

	public static String createRequestUrl(String server, int restPort, String url, HashMap<String, Object> parameters) {
		String fullUrl = ("http://" + server + ":" + restPort + url);
		if (parameters == null || parameters.isEmpty()) {
			return fullUrl;
		}

		return (fullUrl + "?" + createQuery(parameters));

	}
}
