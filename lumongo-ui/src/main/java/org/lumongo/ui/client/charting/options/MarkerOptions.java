package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class MarkerOptions extends JavaScriptObject {

	public static MarkerOptions create() {
		return createObject().cast();
	}

	protected MarkerOptions() {
	}

	public final native MarkerOptions setEnabled(boolean enabled) /*-{
        this.enabled = enabled;
        return this;
    }-*/;

	public final native MarkerOptions setSymbol(String symbol) /*-{
        this.symbol = symbol;
        return this;
    }-*/;

	public final native MarkerOptions setRadius(int radius) /*-{
        this.radius = radius;
        return this;
    }-*/;

}