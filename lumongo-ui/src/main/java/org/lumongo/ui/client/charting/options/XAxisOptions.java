package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;
import com.google.gwt.core.client.JsArrayString;

public class XAxisOptions extends JavaScriptObject {

	public static XAxisOptions create() {
		return createObject().cast();
	}

	protected XAxisOptions() {
	}

	public final native XAxisOptions setAllowDecimals(boolean allowDecimals) /*-{
        this.allowDecimals = allowDecimals;
        return this;
    }-*/;

	public final native XAxisOptions setAlternateGridColor(String alternateGridColor) /*-{
        this.alternateGridColor = alternateGridColor;
        return this;
    }-*/;

	public final ChartOptions setCategories(Iterable<String> categories) {
		return setCategories(JSHelper.toJsArray(categories));
	}

	public final native ChartOptions setCategories(JsArrayString categories) /*-{
        this.categories = categories;
        return this;
    }-*/;

	public final native XAxisOptions setCeiling(int ceiling) /*-{
        this.ceiling = ceilingGridColor;
        return this;
    }-*/;

	public final native XAxisOptions setEndOnTick(boolean endOnTick) /*-{
        this.endOnTick = endOnTick;
        return this;
    }-*/;

	public final native XAxisOptions setFloor(int floor) /*-{
        this.floor = floor;
        return this;
    }-*/;

	public final native XAxisOptions setGridLineColor(String gridLineColor) /*-{
        this.gridLineColor = gridLineColor;
        return this;
    }-*/;

	public final native XAxisOptions setGridDashStyle(String gridDashStyle) /*-{
        this.gridDashStyle = gridDashStyle;
        return this;
    }-*/;

	public final native XAxisOptions setGridLineWidth(int gridLineWidth) /*-{
        this.gridLineWidth = gridLineWidth;
        return this;
    }-*/;

	public final native XAxisOptions setGridZIndex(int gridZIndex) /*-{
        this.gridZIndex = gridZIndex;
        return this;
    }-*/;

	public final native XAxisOptions setId(String id) /*-{
        this.id = id;
        return this;
    }-*/;

	public final native XAxisOptions setLineColor(String lineColor) /*-{
        this.lineColor = lineColor;
        return this;
    }-*/;

	public final native XAxisOptions setLineWidth(int lineWidth) /*-{
        this.lineWidth = lineWidth;
        return this;
    }-*/;

	public final native XAxisOptions setLinkedTo(int linkedTo) /*-{
        this.linkedTo = linkedTo;
        return this;
    }-*/;

	public final native XAxisOptions setMax(int max) /*-{
        this.max = max;
        return this;
    }-*/;

	public final native XAxisOptions setMaxPadding(int maxPadding) /*-{
        this.maxPadding = maxPadding;
        return this;
    }-*/;

	public final native XAxisOptions setMin(int min) /*-{
        this.min = min;
        return this;
    }-*/;

	public final native XAxisOptions setMinPadding(int minPadding) /*-{
        this.minPadding = minPadding;
        return this;
    }-*/;

	public final native XAxisOptions setMinRange(int minRange) /*-{
        this.minRange = minRange;
        return this;
    }-*/;

	public final native XAxisOptions setMinTickInterval(int minTickInterval) /*-{
        this.minTickInterval = minTickInterval;
        return this;
    }-*/;

	public final native XAxisOptions setMinorGridLineColor(String minorGridLineColor) /*-{
        this.minorGridLineColor = minorGridLineColor;
        return this;
    }-*/;

	public final native XAxisOptions setMinorGridLineDashStyle(String minorGridLineDashStyle) /*-{
        this.minorGridLineDashStyle = minorGridLineDashStyle;
        return this;
    }-*/;

	public final native XAxisOptions setMinorGridLineWidth(int minorGridLineWidth) /*-{
        this.minorGridLineWidth = minorGridLineWidth;
        return this;
    }-*/;

	public final native XAxisOptions setMinorTickColor(String minorTickColor) /*-{
        this.minorTickColor = minorTickColor;
        return this;
    }-*/;

	public final native XAxisOptions setMinorTickInterval(String minorTickInterval) /*-{
        this.minorTickInterval = minorTickInterval;
        return this;
    }-*/;

	public final native XAxisOptions setMinorTickInterval(int minorTickInterval) /*-{
        this.minorTickInterval = minorTickInterval;
        return this;
    }-*/;

	public final native XAxisOptions setMinorTickInterval(double minorTickInterval) /*-{
        this.minorTickInterval = minorTickInterval;
        return this;
    }-*/;

	public final native XAxisOptions setMinorTickLength(int minorTickLength) /*-{
        this.minorTickLength = minorTickLength;
        return this;
    }-*/;

	public final native XAxisOptions setMinorTickPosition(String minorTickPosition) /*-{
        this.minorTickPositionLength = minorTickPosition;
        return this;
    }-*/;

	public final native XAxisOptions setMinorTickWidth(int minorTickWidth) /*-{
        this.minorTickWidth = minorTickWidth;
        return this;
    }-*/;

	public final native XAxisOptions setOffset(int offset) /*-{
        this.offset = offset;
        return this;
    }-*/;

	public final native XAxisOptions setOpposite(boolean opposite) /*-{
        this.opposite = opposite;
    }-*/;

	public final native XAxisOptions setReversed(boolean reversed) /*-{
        this.reversed = reversed;
    }-*/;

	public final native XAxisOptions setShowEmpty(boolean showEmpty) /*-{
        this.showEmpty = showEmpty;
    }-*/;

	public final native XAxisOptions setShowFirstLabel(boolean showFirstLabel) /*-{
        this.showFirstLabel = showFirstLabel;
    }-*/;

	public final native XAxisOptions setShowLastLabel(boolean showLastLabel) /*-{
        this.showLastLabel = showLastLabel;
    }-*/;

	public final native XAxisOptions setStartOfWeek(int startOfWeek) /*-{
        this.startOfWeek = startOfWeekWeek;
    }-*/;

	public final native XAxisOptions setStartOnTick(boolean startOnTick) /*-{
        this.startOnTick = startOnTick;
    }-*/;

	public final native XAxisOptions setTickAmount(int tickAmount) /*-{
        this.tickAmount = tickAmount;
    }-*/;

	public final native XAxisOptions setTickColor(String tickColor) /*-{
        this.tickColor = tickColor;
    }-*/;

	public final native XAxisOptions setTickInterval(int tickInterval) /*-{
        this.tickInterval = tickInterval;
    }-*/;

	public final native XAxisOptions setTickLength(int tickLength) /*-{
        this.tickLength = tickLength;
    }-*/;

	public final native XAxisOptions setTickPixelInterval(int tickPixelInterval) /*-{
        this.tickPixelInterval = tickPixelInterval;
    }-*/;

	public final native XAxisOptions setTickWidth(int tickWidth) /*-{
        this.tickWidth = tickWidth;
    }-*/;

	public final native XAxisOptions setTitle(XAxisTitle title) /*-{
        this.title = title;
    }-*/;

	public final XAxisOptions setType(AxisType axisType) {
		return setType(axisType.getName());
	}

	protected final native XAxisOptions setType(String type) /*-{
        this.type = type;
    }-*/;

	public final native TooltipOptions setDateTimeLabelFormats(DateTimeLabelFormats dateTimeLabelFormats)  /*-{
        this.dateTimeLabelFormats = dateTimeLabelFormats;
        return this;
    }-*/;

}