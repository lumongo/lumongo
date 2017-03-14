package org.lumongo.ui.client.highlighter;

import com.google.gwt.user.client.ui.Widget;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

/**
 * Created by Payam Meyer on 3/14/17.
 * @author pmeyer
 */
@JsType(namespace = JsPackage.GLOBAL, isNative = true)
public class HighlightBlock {

	public HighlightBlock() {
	}

	@JsMethod
	public static native void f(Widget block);

}
