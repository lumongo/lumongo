package org.lumongo.ui.client.charting.charts;

import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.charting.selection.handler.DataSelectionHandler;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by Payam Meyer on 9/30/16.
 * @author pmeyer
 */
public abstract class ChartBuilder {

	protected Map<String, Serializable> data;
	protected List<String> colors;
	protected Integer height;
	protected String chartTitle;
	protected String dataSource;
	protected DataSelectionHandler handler;
	protected boolean selectable;
	protected boolean xAxisAllowDecimals = true, yAxisAllowDecimals = true;

	public ChartBuilder setXAxisAllowDecimals(boolean allowDecimals) {
		this.xAxisAllowDecimals = allowDecimals;
		return this;
	}

	public ChartBuilder setYAxisAllowDecimals(boolean allowDecimals) {
		this.yAxisAllowDecimals = allowDecimals;
		return this;
	}

	public ChartBuilder setData(Map<String, Serializable> data) {
		this.data = data;
		return this;
	}

	public ChartBuilder setColors(List<String> colors) {
		this.colors = colors;
		return this;
	}

	public ChartBuilder setHeight(Integer height) {
		this.height = height;
		return this;
	}

	public ChartBuilder setSelectionHandler(DataSelectionHandler handler) {
		this.handler = handler;
		return this;
	}

	public ChartBuilder setChartTitle(String chartTitle) {
		this.chartTitle = chartTitle;
		return this;
	}

	public ChartBuilder setDataSource(String dataSource) {
		this.dataSource = dataSource;
		return this;
	}

	public ChartBuilder setSelectable(boolean selectable) {
		this.selectable = selectable;
		return this;
	}

	public abstract Highcharts build();

}
