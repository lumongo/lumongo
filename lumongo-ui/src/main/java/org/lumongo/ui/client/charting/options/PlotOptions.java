package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class PlotOptions extends JavaScriptObject {

	public static PlotOptions create() {
		return createObject().cast();
	}

	protected PlotOptions() {
	}

	public final native PlotOptions setArea(AreaOptions area) /*-{
        this.area = area;
        return this;
    }-*/;

	public final native PlotOptions setBar(BarOptions bar) /*-{
        this.bar = bar;
        return this;
    }-*/;

	public final native PlotOptions setPie(PieOptions pie) /*-{
        this.pie = pie;
        return this;
    }-*/;

	public final native PlotOptions setColumn(ColumnOptions column) /*-{
        this.column = column;
        return this;
    }-*/;

	public final native PlotOptions setSeries(SeriesOptions series) /*-{
        this.series = series;
        return this;
    }-*/;

}