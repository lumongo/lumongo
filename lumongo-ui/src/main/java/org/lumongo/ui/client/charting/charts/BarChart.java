package org.lumongo.ui.client.charting.charts;

import com.google.gwt.core.client.GWT;
import com.google.gwt.core.client.JsArrayMixed;
import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.charting.handlers.ChartDataSelectionHandler;
import org.lumongo.ui.client.charting.options.*;
import org.lumongo.ui.client.charting.selection.handler.DataSelectionHandler;

/**
 * Created by Payam Meyer on 3/9/2016.
 * @author pmeyer
 */
public class BarChart extends Highcharts implements ChartDataSelectionHandler {
	public static BCBuilder getBuilder() {
		return new BCBuilder();
	}

	public static class BCBuilder extends ChartBuilder {

		@Override
		public Highcharts build() {
			HighchartsOptions options = HighchartsOptions.create();

			ChartOptions chartOptions = ChartOptions.create();
			chartOptions.setPanning(true);
			chartOptions.setPanKey("shift");
			chartOptions.setChartType(ChartType.BAR);
			chartOptions.setZoomType(ZoomType.X);
			chartOptions.setHeight(null == height ? 320 : height);
			options.setChart(chartOptions);

			JsArrayMixed seriesArray = JsArrayMixed.createArray().cast();
			Series series = Series.create();
			JsArrayMixed dataList = JsArrayMixed.createArray().cast();
			for (String key : data.keySet()) {
				dataList.push((Integer) data.get(key));
			}
			series.setData(dataList);
			series.setShowInLegend(false);
			seriesArray.push(series);
			options.setSeries(seriesArray);

			XAxisOptions xAxis = XAxisOptions.create();
			xAxis.setCategories(data.keySet());
			xAxis.setAllowDecimals(false);
			options.setXAxis(xAxis);

			YAxisOptions yAxis = YAxisOptions.create();
			yAxis.setTitle(YAxisTitle.create().setText("Count"));
			yAxis.setMin(0);
			yAxis.setAllowDecimals(super.yAxisAllowDecimals);
			options.setYAxis(yAxis);

			options.setTitle(TitleOptions.create().setText(chartTitle));

			TooltipOptions tooltip = TooltipOptions.create();
			tooltip.setFormatter((x, y, series1) -> x + ": " + y);
			options.setTooltip(tooltip);

			if (null != colors) {
				options.setColors(colors);
			}

			options.setExporting(ExportingOptions.create());
			options.getExporting().setURL(GWT.getHostPageBaseURL() + "highcharts/export");
			options.getExporting().setFilename(chartTitle);
			options.getExporting().setEnabled(true);

			BarChart chart = new BarChart(null == height ? 320 : height);
			if (null != handler) {
				PlotOptions plotOptions = PlotOptions.create();
				SeriesOptions seriesOptions = SeriesOptions.create();
				seriesOptions.setPoint(PointOptions.create().setEvents(SeriesEventOptions.create().setClick(chart)));
				seriesOptions.setCursor(CursorType.POINTER);
				plotOptions.setSeries(seriesOptions);
				options.setPlotOptions(plotOptions);
				chart.setSelectionHandler(handler);
			}
			chart.setHighchartOptions(options);

			chart.setDataSource(dataSource);
			return chart;
		}
	}

	private DataSelectionHandler selectionHandler;
	private String dataSource;

	private BarChart(Integer height) {
		super(height);
	}

	public void setSelectionHandler(DataSelectionHandler selectionHandler) {
		this.selectionHandler = selectionHandler;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	@Override
	public boolean onClick(String category, Integer data) {
		GWT.log("Chart Clicked: " + category + "::" + data);
		if (null != selectionHandler)
			selectionHandler.handleDataSelection(dataSource, category, data);
		return false;
	}

}
