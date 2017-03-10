package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;
import com.google.gwt.core.client.JsArrayMixed;

public class AreaChartData extends JavaScriptObject {

	public static AreaChartData create() {
		return createObject().cast();
	}

	protected AreaChartData() {
	}

	public native final AreaChartData setName(String name) /*-{
        this.name = name;
        return this;
    }-*/;

	public final native AreaChartData setData(JsArrayMixed data) /*-{
        this.data = data;
        return this;
    }-*/;

}
