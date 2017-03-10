package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;
import com.google.gwt.core.client.JsArrayMixed;
import com.google.gwt.core.client.JsArrayString;

public class Series extends JavaScriptObject {

	public static Series create() {
		return createObject().cast();
	}

	protected Series() {
	}

	public final native Series setData(JsArrayMixed data)/*-{
        this.data = data;
    }-*/;

	public final native JsArrayMixed getData() /*-{
        return this.data;
    }-*/;

	public final native Series setMapData(JsArrayMixed mapData)/*-{
        this.mapData = mapData;
    }-*/;

	public final native Series setId(String id) /*-{
        this.id = id;
        return this;
    }-*/;

	public final native Series setIndex(int index) /*-{
        this.index = index;
        return this;
    }-*/;

	public final native Series setLegendIndex(int legendIndex) /*-{
        this.legendIndex = legendIndex;
        return this;
    }-*/;

	public final native Series setName(String name) /*-{
        this.name = name;
        return this;
    }-*/;

	public final native String getName() /*-{
        return this.name;
    }-*/;

	public final Series setType(ChartType chartType) {
		return setType(chartType.getName());
	}

	protected final native Series setType(String type) /*-{
        this.type = type;
        return this;
    }-*/;

	public final native Series setZIndex(int zIndex) /*-{
        this.zIndex = zIndex;
        return this;
    }-*/;

	public final native Series setPointStart(double pointStart) /*-{
        this.pointStart = pointStart;
        return this;
    }-*/;

	public final native Series setPointInterval(int pointInterval) /*-{
        this.pointInterval = pointInterval;
        return this;
    }-*/;

	public final native Series setShowInLegend(boolean showInLegend) /*-{
        this.showInLegend = showInLegend;
        return this;
    }-*/;

	public final native Series setJoinBy(JsArrayString joinBy) /*-{
        this.joinBy = joinBy;
        return this;
    }-*/;

	public final native Series setDataLabels(DataLabelsOptions dataLabels) /*-{
        this.dataLabels = dataLabels;
        return this;
    }-*/;

}