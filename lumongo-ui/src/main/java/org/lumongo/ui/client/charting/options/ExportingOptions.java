package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class ExportingOptions extends JavaScriptObject {

	public static ExportingOptions create() {
		return createObject().cast();
	}

	protected ExportingOptions() {
	}

	public final native ExportingOptions setEnabled(boolean enabled) /*-{
        this.enabled = enabled;
        return this;
    }-*/;

	public final native ChartOptions getChart() /*-{
        return this.chart;
    }-*/;

	public final native ExportingOptions setFilename(String filename) /*-{
        this.filename = filename;
        return this;
    }-*/;

	public final native ExportingOptions setScale(int scale) /*-{
        this.scale = scale;
        return this;
    }-*/;

	public final native ExportingOptions setSourceWidth(int sourceWidth) /*-{
        this.sourceWidth = sourceWidth;
        return this;
    }-*/;

	public final native ExportingOptions setSourceHeight(int sourceHeight) /*-{
        this.sourceHeight = sourceHeight;
        return this;
    }-*/;

	public final native ExportingOptions setType(String type) /*-{
        this.type = type;
        return this;
    }-*/;

	public final native ExportingOptions setURL(String url) /*-{
        this.url = url;
        return this;
    }-*/;

	public final native ExportingOptions setWidth(int width) /*-{
        this.width = width;
        return this;
    }-*/;

}
