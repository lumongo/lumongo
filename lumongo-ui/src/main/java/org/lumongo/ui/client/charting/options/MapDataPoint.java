package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

/**
 * Created by mdavis on 5/15/15.
 */
public class MapDataPoint extends JavaScriptObject {

	public static MapDataPoint create() {
		return createObject().cast();
	}

	protected MapDataPoint() {
	}

	public final native MapDataPoint setCode(String code) /*-{
        this.code = code;
        return this;
    }-*/;

	public final native MapDataPoint setValue(double value) /*-{
        this.value = value;
        return this;
    }-*/;

	public final native MapDataPoint setDrilldown(String drilldown) /*-{
        this.drilldown = drilldown;
        return this;
    }-*/;

	public final native MapDataPoint setDisplayName(String displayName) /*-{
        this.displayName = displayName;
        return this;
    }-*/;
}