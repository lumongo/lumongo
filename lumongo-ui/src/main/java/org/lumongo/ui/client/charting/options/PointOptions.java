package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class PointOptions extends JavaScriptObject {

	public static PointOptions create() {
		return createObject().cast();
	}

	protected PointOptions() {
	}

	public final native PointOptions setEvents(JavaScriptObject events) /*-{
        this.events = events;
        return this;
    }-*/;

}