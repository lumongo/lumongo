package org.lumongo.ui.client.highlighter;

import com.google.gwt.dom.client.Element;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

/**
 * Created by Payam Meyer on 3/14/17.
 * @author pmeyer
 */
@JsType(namespace = JsPackage.GLOBAL, isNative = true)
public class Highlight {

	@JsMethod
	public native void configure(HighlightOptions highlightOptions);

	@JsMethod
	public native void initHighlightingOnLoad();

	@JsMethod
	public static native void e(Element block);

}
