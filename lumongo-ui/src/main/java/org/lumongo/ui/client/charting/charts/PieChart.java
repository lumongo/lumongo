package org.lumongo.ui.client.charting.charts;

import com.google.gwt.core.client.GWT;
import com.google.gwt.core.client.JsArrayMixed;
import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.charting.handlers.ChartDataSelectionHandler;
import org.lumongo.ui.client.charting.options.ChartOptions;
import org.lumongo.ui.client.charting.options.ChartType;
import org.lumongo.ui.client.charting.options.CursorType;
import org.lumongo.ui.client.charting.options.ExportingOptions;
import org.lumongo.ui.client.charting.options.HighchartsOptions;
import org.lumongo.ui.client.charting.options.PieEventOptions;
import org.lumongo.ui.client.charting.options.PieOptions;
import org.lumongo.ui.client.charting.options.PlotOptions;
import org.lumongo.ui.client.charting.options.PointOptions;
import org.lumongo.ui.client.charting.options.Series;
import org.lumongo.ui.client.charting.options.TitleOptions;
import org.lumongo.ui.client.charting.selection.handler.DataSelectionHandler;

/**
 * Created by Payam Meyer on 3/21/2016.
 * @author pmeyer
 */
public class PieChart extends Highcharts implements ChartDataSelectionHandler {
	public static PCBuilder getBuilder() {
		return new PCBuilder();
	}

	public static class PCBuilder extends ChartBuilder {

		public PieChart build() {
			HighchartsOptions options = HighchartsOptions.create();

			ChartOptions chartOptions = ChartOptions.create();
			chartOptions.setChartType(ChartType.PIE);
			options.setChart(chartOptions);

			JsArrayMixed seriesArray = JsArrayMixed.createArray().cast();
			Series series = Series.create();
			series.setName(chartTitle);
			JsArrayMixed dataList = JsArrayMixed.createArray().cast();
			for (String key : data.keySet()) {
				//FIXME: There needs to be a limit to the number of entries in a pie chart.
				JsArrayMixed values = JsArrayMixed.createArray().cast();
				values.push(key);
				values.push((Integer) data.get(key));
				dataList.push(values);
			}
			series.setData(dataList);
			seriesArray.push(series);
			options.setSeries(seriesArray);

			options.setTitle(TitleOptions.create().setText(chartTitle));

			if (null != colors && !colors.isEmpty()) {
				options.setColors(colors);
			}

			options.setExporting(ExportingOptions.create());
			options.getExporting().setURL(GWT.getHostPageBaseURL() + "highcharts/export");
			options.getExporting().setFilename(chartTitle);
			options.getExporting().setEnabled(true);

			PieChart chart = new PieChart(null == height ? 320 : height);
			if (null != handler) {
				PlotOptions plotOptions = PlotOptions.create();
				PieOptions pieOptions = PieOptions.create();
				pieOptions.setPoint(PointOptions.create().setEvents(PieEventOptions.create().setClick(chart)));
				pieOptions.setCursor(CursorType.POINTER);
				plotOptions.setPie(pieOptions);
				options.setPlotOptions(plotOptions);
				chart.setDataSelectionHandler(handler);
			}
			chart.setDataSource(dataSource);

			chart.setHighchartOptions(options);
			chart.setDataSource(dataSource);

			return chart;
		}
	}

	private DataSelectionHandler handler;
	private String dataSource;

	private PieChart(Integer height) {
		super(height);
	}

	private void setDataSelectionHandler(DataSelectionHandler handler) {
		this.handler = handler;
	}

	private void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	@Override
	public boolean onClick(String slice, Integer count) {
		GWT.log("Chart Clicked: " + dataSource + "::" + slice + "::" + count);
		if (null != handler)
			handler.handleDataSelection(dataSource, slice, count);
		return null != handler;
	}
}
