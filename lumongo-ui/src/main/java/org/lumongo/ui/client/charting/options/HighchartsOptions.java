package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;
import com.google.gwt.core.client.JsArrayMixed;
import com.google.gwt.core.client.JsArrayString;

public class HighchartsOptions extends JavaScriptObject {

	public static HighchartsOptions create() {
		return createObject().cast();
	}

	protected HighchartsOptions() {
	}

	public final native HighchartsOptions setChart(ChartOptions chart) /*-{
        this.chart = chart;
        return this;
    }-*/;

	public final native ChartOptions getChart() /*-{
        return this.chart;
    }-*/;

	public final native HighchartsOptions setCredits(CreditOptions credits) /*-{
        this.credits = credits;
        return this;
    }-*/;

	public final native CreditOptions getCredits() /*-{
        return this.credits;
    }-*/;

	public final native HighchartsOptions setTitle(TitleOptions title) /*-{
        this.title = title;
        return this;
    }-*/;

	public final native TitleOptions getTitle() /*-{
        return this.title;
    }-*/;

	public final native HighchartsOptions setSubtitle(SubtitleOptions subtitle) /*-{
        this.subtitle = subtitle;
        return this;
    }-*/;

	public final native SubtitleOptions getSubtitle() /*-{
        return this.title;
    }-*/;

	public final native HighchartsOptions setLegend(LegendOptions legend) /*-{
        this.legend = legend;
        return this;
    }-*/;

	public final native SubtitleOptions getLegend() /*-{
        return this.legend;
    }-*/;

	public final native HighchartsOptions setTooltip(TooltipOptions tooltip) /*-{
        this.tooltip = tooltip;
        return this;
    }-*/;

	public final native TooltipOptions getTooltip() /*-{
        return this.tooltip;
    }-*/;

	public final native HighchartsOptions setColorAxis(ColorAxisOptions colorAxis) /*-{
        this.colorAxis = colorAxis;
        return this;
    }-*/;

	public final native ColorAxisOptions getColorAxis() /*-{
        return this.colorAxis;
    }-*/;

	public final HighchartsOptions setColors(Iterable<String> colors) {
		return setColors(JSHelper.toJsArray(colors));
	}

	protected final native HighchartsOptions setColors(JsArrayString colors) /*-{
        this.colors = colors;
        return this;
    }-*/;

	public final native JsArrayString getColors() /*-{
        return this.colors;
    }-*/;

	public final native HighchartsOptions setXAxis(XAxisOptions xAxis) /*-{
        this.xAxis = xAxis;
        return this;
    }-*/;

	public final native XAxisOptions getXAxis() /*-{
        return this.xAxis;
    }-*/;

	public final native HighchartsOptions setYAxis(YAxisOptions yAxis) /*-{
        this.yAxis = yAxis;
        return this;
    }-*/;

	public final native YAxisOptions getYAxis() /*-{
        return this.yAxis;
    }-*/;

	public final native HighchartsOptions setSeries(JsArrayMixed series) /*-{
        this.series = series;
    }-*/;

	public final native JsArrayMixed getSeries() /*-{
        return this.series;
    }-*/;

	public final native HighchartsOptions setPlotOptions(PlotOptions plotOptions) /*-{
        this.plotOptions = plotOptions;
        return this;
    }-*/;

	public final native PlotOptions getPlotOptions() /*-{
        return this.plotOptions;
    }-*/;

	public final native HighchartsOptions setExporting(ExportingOptions exporting) /*-{
        this.exporting = exporting;
        return this;
    }-*/;

	public final native ExportingOptions getExporting() /*-{
        return this.exporting;
    }-*/;

	public final native HighchartsOptions setExporting(MapNavigationOptions mapNavigation) /*-{
        this.mapNavigation = mapNavigation;
        return this;
    }-*/;

	public final native MapNavigationOptions getMapNavigation() /*-{
        return this.mapNavigation;
    }-*/;

	public final native HighchartsOptions setMapNavigation(MapNavigationOptions mapNavigation) /*-{
        this.mapNavigation = mapNavigation;
        return this;
    }-*/;
}
