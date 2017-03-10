package org.lumongo.ui.client.charting.charts;

import com.google.gwt.core.client.GWT;
import com.google.gwt.core.client.JsArrayMixed;
import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.charting.handlers.ChartDataSelectionHandler;
import org.lumongo.ui.client.charting.options.*;
import org.lumongo.ui.client.charting.selection.handler.DataSelectionHandler;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Created by Payam Meyer on 4/10/2016.
 * @author pmeyer
 */
public class ColumnChart extends Highcharts implements ChartDataSelectionHandler {
	public static CCBuilder getBuilder() {
		return new CCBuilder();
	}

	public static class CCBuilder extends ChartBuilder {
		private String yAxisLabel = "Values"; // default value
		private String xAxisLabel = "";

		public CCBuilder setYAxisLabel(String yAxisLabel) {
			this.yAxisLabel = yAxisLabel;
			return this;
		}

		public CCBuilder setXAxisLabel(String xAxisLabel) {
			this.xAxisLabel = xAxisLabel;
			return this;
		}

		public CCBuilder setNamedData(Map<String, Serializable> data) {
			this.data = data;
			return this;
		}

		public CCBuilder addNamedData(String name, Map<String, Serializable> data) {
			if (null == this.data)
				this.data = new HashMap<>();
			if (!this.data.containsKey(name))
				this.data.put(name, new HashMap<>());
			((Map<String, Serializable>) this.data.get(name)).putAll(data);
			return this;
		}

		public ColumnChart build() {
			HighchartsOptions options = HighchartsOptions.create();

			ChartOptions chartOptions = ChartOptions.create();
			//chartOptions.setPanning(true);
			//chartOptions.setPanKey("shift");
			chartOptions.setChartType(ChartType.COLUMN);
			chartOptions.setZoomType(ZoomType.X);
			//chartOptions.setHeight(null == height ? 320 : height);
			options.setChart(chartOptions);

			TitleOptions titleOptions = TitleOptions.create();
			titleOptions.setText(chartTitle);
			options.setTitle(titleOptions);

			if (null != colors) {
				options.setColors(colors);
			}

			LinkedHashSet<String> categories = new LinkedHashSet<>();
			for (String key : data.keySet()) {
				for (String x : ((Map<String, Integer>) this.data.get(key)).keySet()) {
					categories.add(x);
				}
			}

			JsArrayMixed d = JsArrayMixed.createArray().cast();
			for (String dataName : data.keySet()) {
				Series entry = Series.create();
				entry.setName(dataName);
				JsArrayMixed entries = JsArrayMixed.createArray().cast();
				for (String value : categories) {
					if (((Map<String, Integer>) data.get(dataName)).containsKey(value))
						entries.push(((Map<String, Integer>) data.get(dataName)).get(value));
					else
						entries.push(0);
				}
				entry.setData(entries);
				entry.setShowInLegend(true);
				d.push(entry);
			}
			options.setSeries(d);

			XAxisOptions xAxis = XAxisOptions.create();
			xAxis.setCategories(categories);
			xAxis.setAllowDecimals(false);
			xAxis.setTitle(XAxisTitle.create().setText(xAxisLabel));
			options.setXAxis(xAxis);

			YAxisOptions yAxis = YAxisOptions.create();
			yAxis.setTitle(YAxisTitle.create().setText(yAxisLabel));
			yAxis.setMin(0);
			yAxis.setAllowDecimals(false);
			options.setYAxis(yAxis);

			TooltipOptions tooltip = TooltipOptions.create();
			//tooltip.setFormatter((x, y, series1) -> series1.getName() + " <br/> " + yAxisLabel + ": " + y);
			tooltip.setPointFormat("<span style=\"color:{point.color}\">\u25CF</span> {series.name}: <b>{point.y}</b><br/>");
			options.setTooltip(tooltip);

			PlotOptions plotOptions = PlotOptions.create();
			ColumnOptions columnOptions = ColumnOptions.create();
			columnOptions.setMaxColumnWidth(100);
			plotOptions.setColumn(columnOptions);
			options.setPlotOptions(plotOptions);

			options.setExporting(ExportingOptions.create());
			options.getExporting().setURL(GWT.getHostPageBaseURL() + "highcharts/export");
			options.getExporting().setFilename(chartTitle);
			options.getExporting().setEnabled(true);

			ColumnChart cc = new ColumnChart(height);

			if (null != handler) {
				PointOptions po = PointOptions.create();
				ColumnEventOptions ceo = ColumnEventOptions.create();
				ceo.setClick(cc);
				po.setEvents(ceo);
				columnOptions.setPointOptions(po);
				cc.setHandler(handler);
			}

			cc.setHighchartOptions(options);
			cc.setDataSource(dataSource);

			return cc;
		}
	}

	private DataSelectionHandler handler;
	private String dataSource;

	private ColumnChart(Integer height) {
		super(height);
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public void setHandler(DataSelectionHandler handler) {
		this.handler = handler;
	}

	@Override
	public boolean onClick(String x, Integer y) {
		GWT.log("Chart Clicked: " + x + "::" + y);
		handler.handleDataSelection(dataSource, x, y);
		return false;
	}
}
