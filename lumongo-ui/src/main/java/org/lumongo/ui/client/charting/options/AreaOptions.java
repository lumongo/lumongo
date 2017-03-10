package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class AreaOptions extends JavaScriptObject {

	public static AreaOptions create() {
		return createObject().cast();
	}

	protected AreaOptions() {
	}

	public native final AreaOptions setPointStart(Double pointStart) /*-{
        this.pointStart = pointStart;
        return this;
    }-*/;

	public final native AreaOptions setMarker(MarkerOptions marker) /*-{
        this.marker = marker;
        return this;
    }-*/;

}
