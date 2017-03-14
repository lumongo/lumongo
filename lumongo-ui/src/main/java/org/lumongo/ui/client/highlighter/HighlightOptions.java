package org.lumongo.ui.client.highlighter;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

/**
 * Created by Payam Meyer on 3/14/17.
 * @author pmeyer
 */
@JsType(namespace = JsPackage.GLOBAL, isNative = true, name = "Object")
public class HighlightOptions {

	public String tabReplace;
	public boolean userBR;

}
