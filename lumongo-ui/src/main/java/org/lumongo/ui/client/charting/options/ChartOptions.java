package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class ChartOptions extends JavaScriptObject {

	public static ChartOptions create() {
		return createObject().cast();
	}

	protected ChartOptions() {
	}

	public final ChartOptions setChartType(ChartType chartType) {
		return setChartType(chartType.getName());
	}

	protected final native ChartOptions setChartType(String type) /*-{
        this.type = type;
        return this;
    }-*/;

	public final native ChartOptions setHeight(int height) /*-{
        this.height = height;
        return this;
    }-*/;

	public final native ChartOptions setRenderTo(String renderTo) /*-{
        this.renderTo = renderTo;
        return this;
    }-*/;

	public final HighchartsOptions setZoomType(ZoomType zoomType) {
		return setZoomType(zoomType.getName());
	}

	protected final native HighchartsOptions setZoomType(String zoomType) /*-{
        this.zoomType = zoomType;
        return this;
    }-*/;

	public final native HighchartsOptions setPanning(boolean panning) /*-{
        this.panning = panning;
        return this;
    }-*/;

	public final native HighchartsOptions setPanKey(String panKey) /*-{
        this.panKey = panKey;
        return this;
    }-*/;

	public final native HighchartsOptions setEvents(ChartEventsOptions events) /*-{
        this.events = events;
        return this;
    }-*/;

}