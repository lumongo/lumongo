package org.lumongo.ui.client.highlighter;

import com.google.gwt.core.client.GWT;
import com.google.gwt.resources.client.ClientBundle;
import com.google.gwt.resources.client.TextResource;

/**
 * Created by Payam Meyer on 3/14/17.
 * @author pmeyer
 */
public interface HighlightBundle extends ClientBundle {

	HighlightBundle INSTANCE = GWT.create(HighlightBundle.class);

	@Source("resource/highlight.min.js")
	TextResource highlight();

	@Source("resource/default.min.css")
	TextResource style();

}
