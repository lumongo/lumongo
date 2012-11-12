package org.lumongo.util;


public class StringUtil {
	private StringUtil() {

	}

	public static String join(char delimiter, String... values) {
		return join(delimiter + "", values);
	}

	public static String join(String delimiter, String... values) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String value : values) {
			if (first) {
				first = false;
			}
			else {
				sb.append(delimiter);
			}
			sb.append(value);
		}
		return sb.toString();
	}
}
