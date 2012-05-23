package org.lumongo.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;

import org.lumongo.LumongoConstants;

public class HttpHelper {
	public static String createQuery(HashMap<String, String> parameters) {
		
		StringBuilder sb = new StringBuilder();
		
		for (String key : parameters.keySet()) {
			if (sb.length() > 0) {
				sb.append('&');
			}
			
			sb.append(key);
			sb.append('=');
			try {
				sb.append(URLEncoder.encode(parameters.get(key), LumongoConstants.UTF8));
			}
			catch (UnsupportedEncodingException e) {
				//should not be possible
				throw new RuntimeException(e);
			}
		}
		return sb.toString();
	}
	
	public static String createRequestUrl(String server, int restPort, String url, HashMap<String, String> parameters) {
		String fullUrl = ("http://" + server + ":" + restPort + url);
		if (parameters == null || parameters.isEmpty()) {
			return fullUrl;
		}
		
		return (fullUrl + "?" + createQuery(parameters));
		
	}
}
