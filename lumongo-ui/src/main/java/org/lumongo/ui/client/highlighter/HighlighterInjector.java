package org.lumongo.ui.client.highlighter;

import com.google.gwt.core.client.GWT;
import com.google.gwt.core.client.ScriptInjector;
import com.google.gwt.dom.client.StyleInjector;

/**
 * Created by Payam Meyer on 3/7/2016.
 * @author millm
 */
public class HighlighterInjector {

	public static void inject() {
		ScriptInjector.fromString(HighlightBundle.INSTANCE.highlight().getText()).setWindow(ScriptInjector.TOP_WINDOW).inject();
		StyleInjector.injectStylesheetAtStart(HighlightBundle.INSTANCE.style().getText());
		GWT.log("Injected highlighter.");
	}

}
