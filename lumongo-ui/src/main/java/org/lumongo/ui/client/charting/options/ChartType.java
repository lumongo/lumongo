package org.lumongo.ui.client.charting.options;

public enum ChartType {
	AREA("area"),
	AREA_RANGE("arearange"),
	AREA_SPLINE("areaspline"),
	AREA_SPLINE_RANGE("areasplinerange"),
	BAR("bar"),
	BOXPLOT("boxplot"),
	BUBBLE("bubble"),
	COLUMN("column"),
	COLUMN_RANGE("columnrange"),
	ERROR_BAR("errorbar"),
	FUNNEL("funnel"),
	GAUGE("gauge"),
	HEATMAP("heatmap"),
	LINE("line"),
	PIE("pie"),
	POLYGON("polygon"),
	PYRAMID("pyramid"),
	SCATTER("scatter"),
	SERIES("series"),
	SOLID_GAUGE("solidgauge"),
	SPLINE("spline"),
	TREEMAP("treemap"),
	WATERFALL("waterfall");

	private final String name;

	ChartType(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
