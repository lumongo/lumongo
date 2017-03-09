package org.lumongo.ui.client.charting.charts;

import com.google.gwt.core.client.GWT;
import com.google.gwt.core.client.JsArrayMixed;
import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.charting.handlers.ChartDataSelectionHandler;
import org.lumongo.ui.client.charting.options.*;
import org.lumongo.ui.client.charting.selection.handler.DataSelectionHandler;

import java.util.List;

/**
 * Created by Payam Meyer on 3/9/2016.
 * @author pmeyer
 */
public class AreaChart extends Highcharts implements ChartDataSelectionHandler {
	public static ACBuilder getBuilder() {
		return new ACBuilder();
	}

	public static class ACBuilder extends ChartBuilder {

		@Override
		public Highcharts build() {
			HighchartsOptions options = HighchartsOptions.create();

			ChartOptions chartOptions = ChartOptions.create();
			chartOptions.setChartType(ChartType.AREA);
			options.setChart(chartOptions);

			XAxisOptions xAxis = XAxisOptions.create();
			xAxis.setAllowDecimals(super.xAxisAllowDecimals);
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

			PlotOptions plotOptions = PlotOptions.create();

			AreaOptions areaOptions = AreaOptions.create();
			MarkerOptions markerOptions = MarkerOptions.create();
			markerOptions.setEnabled(false);
			markerOptions.setSymbol("circle");
			markerOptions.setRadius(2);
			areaOptions.setMarker(markerOptions);
			areaOptions.setPointStart(0.0);
			plotOptions.setArea(areaOptions);

			JsArrayMixed seriesArray = JsArrayMixed.createArray().cast();
			Series series = Series.create();
			JsArrayMixed dataList = JsArrayMixed.createArray().cast();
			for (String key : data.keySet()) {
				AreaChartData areaData = AreaChartData.create();
				areaData.setName(key);
				JsArrayMixed dataValues = JsArrayMixed.createArray().cast();
				for (Double value : (List<Double>) data.get(key)) {
					dataValues.push(value);
				}
				areaData.setData(dataValues);
				seriesArray.push(areaData);
			}

			series.setData(dataList);
			series.setShowInLegend(false);

			//seriesArray.push(series);
			options.setSeries(seriesArray);

			options.setPlotOptions(plotOptions);

			AreaChart chart = new AreaChart(null == height ? 320 : height);
			if (null != handler) {
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

	private AreaChart(Integer height) {
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
