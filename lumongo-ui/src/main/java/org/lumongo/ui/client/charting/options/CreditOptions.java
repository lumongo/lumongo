package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class CreditOptions extends JavaScriptObject {

	public static CreditOptions create() {
		return createObject().cast();
	}

	protected CreditOptions() {
	}

	public final native HighchartsOptions setEnabled(boolean enabled) /*-{
        this.enabled = enabled;
        return this;
    }-*/;

}