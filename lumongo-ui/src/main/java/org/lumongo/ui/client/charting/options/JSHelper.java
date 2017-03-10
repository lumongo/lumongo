package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JsArrayString;

public class JSHelper {

	public static JsArrayString toJsArray(Iterable<String> values) {
		JsArrayString array = JsArrayString.createArray().cast();
		for (String value : values) {
			array.push(value);
		}
		return array;
	}
}
