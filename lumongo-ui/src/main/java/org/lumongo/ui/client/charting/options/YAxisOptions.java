package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;
import com.google.gwt.core.client.JsArrayString;

public class YAxisOptions extends JavaScriptObject {

	public static YAxisOptions create() {
		return createObject().cast();
	}

	protected YAxisOptions() {
	}

	public final native YAxisOptions setAllowDecimals(boolean allowDecimals) /*-{
        this.allowDecimals = allowDecimals;
        return this;
    }-*/;

	public final native YAxisOptions setAlternateGridColor(String alternateGridColor) /*-{
        this.alternateGridColor = alternateGridColor;
        return this;
    }-*/;

	public final ChartOptions setCategories(Iterable<String> categories) {
		return setCategories(JSHelper.toJsArray(categories));
	}

	protected final native ChartOptions setCategories(JsArrayString categories) /*-{
        this.categories = categories;
        return this;
    }-*/;

	public final native YAxisOptions setCeiling(int ceiling) /*-{
        this.ceiling = ceilingGridColor;
        return this;
    }-*/;

	public final native YAxisOptions setEndOnTick(boolean endOnTick) /*-{
        this.endOnTick = endOnTick;
        return this;
    }-*/;

	public final native YAxisOptions setFloor(int floor) /*-{
        this.floor = floor;
        return this;
    }-*/;

	public final native YAxisOptions setGridLineColor(String gridLineColor) /*-{
        this.gridLineColor = gridLineColor;
        return this;
    }-*/;

	public final native YAxisOptions setGridDashStyle(String gridDashStyle) /*-{
        this.gridDashStyle = gridDashStyle;
        return this;
    }-*/;

	public final native YAxisOptions setGridLineWidth(int gridLineWidth) /*-{
        this.gridLineWidth = gridLineWidth;
        return this;
    }-*/;

	public final native YAxisOptions setGridZIndex(int gridZIndex) /*-{
        this.gridZIndex = gridZIndex;
        return this;
    }-*/;

	public final native YAxisOptions setId(String id) /*-{
        this.id = id;
        return this;
    }-*/;

	public final native YAxisOptions setLineColor(String lineColor) /*-{
        this.lineColor = lineColor;
        return this;
    }-*/;

	public final native YAxisOptions setLineWidth(int lineWidth) /*-{
        this.lineWidth = lineWidth;
        return this;
    }-*/;

	public final native YAxisOptions setLinkedTo(int linkedTo) /*-{
        this.linkedTo = linkedTo;
        return this;
    }-*/;

	public final native YAxisOptions setMax(int max) /*-{
        this.max = max;
        return this;
    }-*/;

	public final native YAxisOptions setMaxPadding(int maxPadding) /*-{
        this.maxPadding = maxPadding;
        return this;
    }-*/;

	public final native YAxisOptions setMin(int min) /*-{
        this.min = min;
        return this;
    }-*/;

	public final native YAxisOptions setMinPadding(int minPadding) /*-{
        this.minPadding = minPadding;
        return this;
    }-*/;

	public final native YAxisOptions setMinRange(int minRange) /*-{
        this.minRange = minRange;
        return this;
    }-*/;

	public final native YAxisOptions setMinTickInterval(int minTickInterval) /*-{
        this.minTickInterval = minTickInterval;
        return this;
    }-*/;

	public final native YAxisOptions setMinorGridLineColor(String minorGridLineColor) /*-{
        this.minorGridLineColor = minorGridLineColor;
        return this;
    }-*/;

	public final native YAxisOptions setMinorGridLineDashStyle(String minorGridLineDashStyle) /*-{
        this.minorGridLineDashStyle = minorGridLineDashStyle;
        return this;
    }-*/;

	public final native YAxisOptions setMinorGridLineWidth(int minorGridLineWidth) /*-{
        this.minorGridLineWidth = minorGridLineWidth;
        return this;
    }-*/;

	public final native YAxisOptions setMinorTickColor(String minorTickColor) /*-{
        this.minorTickColor = minorTickColor;
        return this;
    }-*/;

	public final native YAxisOptions setMinorTickInterval(String minorTickInterval) /*-{
        this.minorTickInterval = minorTickInterval;
        return this;
    }-*/;

	public final native YAxisOptions setMinorTickInterval(int minorTickInterval) /*-{
        this.minorTickInterval = minorTickInterval;
        return this;
    }-*/;

	public final native YAxisOptions setMinorTickInterval(double minorTickInterval) /*-{
        this.minorTickInterval = minorTickInterval;
        return this;
    }-*/;

	public final native YAxisOptions setMinorTickLength(int minorTickLength) /*-{
        this.minorTickLength = minorTickLength;
        return this;
    }-*/;

	public final native YAxisOptions setMinorTickPosition(String minorTickPosition) /*-{
        this.minorTickPositionLength = minorTickPosition;
        return this;
    }-*/;

	public final native YAxisOptions setMinorTickWidth(int minorTickWidth) /*-{
        this.minorTickWidth = minorTickWidth;
        return this;
    }-*/;

	public final native YAxisOptions setOffset(int offset) /*-{
        this.offset = offset;
        return this;
    }-*/;

	public final native YAxisOptions setOpposite(boolean opposite) /*-{
        this.opposite = opposite;
    }-*/;

	public final native YAxisOptions setReversed(boolean reversed) /*-{
        this.reversed = reversed;
    }-*/;

	public final native YAxisOptions setShowEmpty(boolean showEmpty) /*-{
        this.showEmpty = showEmpty;
    }-*/;

	public final native YAxisOptions setShowFirstLabel(boolean showFirstLabel) /*-{
        this.showFirstLabel = showFirstLabel;
    }-*/;

	public final native YAxisOptions setShowLastLabel(boolean showLastLabel) /*-{
        this.showLastLabel = showLastLabel;
    }-*/;

	public final native YAxisOptions setStartOfWeek(int startOfWeek) /*-{
        this.startOfWeek = startOfWeekWeek;
    }-*/;

	public final native YAxisOptions setStartOnTick(boolean startOnTick) /*-{
        this.startOnTick = startOnTick;
    }-*/;

	public final native YAxisOptions setTickAmount(int tickAmount) /*-{
        this.tickAmount = tickAmount;
    }-*/;

	public final native YAxisOptions setTickColor(String tickColor) /*-{
        this.tickColor = tickColor;
    }-*/;

	public final native YAxisOptions setTickInterval(int tickInterval) /*-{
        this.tickInterval = tickInterval;
    }-*/;

	public final native YAxisOptions setTickLength(int tickLength) /*-{
        this.tickLength = tickLength;
    }-*/;

	public final native YAxisOptions setTickPixelInterval(int tickPixelInterval) /*-{
        this.tickPixelInterval = tickPixelInterval;
    }-*/;

	public final native YAxisOptions setTickWidth(int tickWidth) /*-{
        this.tickWidth = tickWidth;
    }-*/;

	public final native YAxisOptions setTitle(YAxisTitle title) /*-{
        this.title = title;
    }-*/;

	public final YAxisOptions setType(AxisType axisType) {
		return setType(axisType.getName());
	}

	protected final native YAxisOptions setType(String type) /*-{
        this.type = type;
    }-*/;

}