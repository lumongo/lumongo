package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class DataLabelsOptions extends JavaScriptObject {

	public static DataLabelsOptions create() {
		return createObject().cast();
	}

	protected DataLabelsOptions() {
	}

	public final native DataLabelsOptions setEnabled(boolean enabled) /*-{
        this.enabled = enabled;
        return this;
    }-*/;

	public final native DataLabelsOptions setFormat(String format) /*-{
        this.format = format;
        return this;
    }-*/;

}
