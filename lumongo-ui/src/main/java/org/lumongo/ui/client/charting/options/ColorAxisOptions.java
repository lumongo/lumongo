package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class ColorAxisOptions extends JavaScriptObject {

	public static ColorAxisOptions create() {
		return createObject().cast();
	}

	protected ColorAxisOptions() {
	}

	public final native ColorAxisOptions setMin(int min) /*-{
        this.min = min;
        return this;
    }-*/;

	public final native ColorAxisOptions setMax(int max) /*-{
        this.max = max;
        return this;
    }-*/;

	public final ColorAxisOptions setType(AxisType axisType) {
		return setType(axisType.getName());
	}

	protected final native ColorAxisOptions setType(String type) /*-{
        this.type = type;
        return this;
    }-*/;

	public final native ColorAxisOptions setReversed(boolean reversed) /*-{
        this.reversed = reversed;
    }-*/;

	public final native ColorAxisOptions setMinColor(String minColor) /*-{
        this.minColor = minColor;
        return this;
    }-*/;

	public final native ColorAxisOptions setMaxColor(String maxColor) /*-{
        this.maxColor = maxColor;
        return this;
    }-*/;

}
