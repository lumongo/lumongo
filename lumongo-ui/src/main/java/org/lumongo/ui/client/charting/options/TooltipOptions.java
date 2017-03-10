package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;
import org.lumongo.ui.client.charting.handlers.TooltipFormatter;

public class TooltipOptions extends JavaScriptObject {

	public static TooltipOptions create() {
		return createObject().cast();
	}

	protected TooltipOptions() {
	}

	public final native TooltipOptions setAnimation(boolean animation) /*-{
        this.animation = animation;
        return this;
    }-*/;

	public final native TooltipOptions setBackgroundColor(String backgroundColor) /*-{
        this.backgroundColor = backgroundColor;
        return this;
    }-*/;

	public final native TooltipOptions setBorderColor(String borderColor) /*-{
        this.borderColor = borderColor;
        return this;
    }-*/;

	public final native TooltipOptions setBorderRadius(int borderRadius) /*-{
        this.borderRadius = borderRadius;
        return this;
    }-*/;

	public final native TooltipOptions setBorderWidth(int borderWidth) /*-{
        this.borderWidth = borderWidth;
        return this;
    }-*/;

	public final native TooltipOptions setEnabled(boolean enabled) /*-{
        this.enabled = enabled;
        return this;
    }-*/;

	public final native TooltipOptions setShadow(boolean shadow) /*-{
        this.shadow = shadow;
        return this;
    }-*/;

	public final native TooltipOptions setShared(boolean shared) /*-{
        this.shared = shared;
        return this;
    }-*/;

	public final native TooltipOptions setUseHTML(boolean useHTML) /*-{
        this.useHTML = useHTML;
        return this;
    }-*/;

	public final native TooltipOptions setDateTimeLabelFormats(DateTimeLabelFormats dateTimeLabelFormats)  /*-{
        this.dateTimeLabelFormats = dateTimeLabelFormats;
        return this;
    }-*/;

	public final native TooltipOptions setFormatter(TooltipFormatter formatter) /*-{
        this.formatter = function () {
            return formatter.@org.lumongo.ui.client.charting.handlers.TooltipFormatter::format(Ljava/lang/String;Ljava/lang/String;Lorg/lumongo/ui/client/charting/options/Series;)(this.x.toString(), this.y.toString(), this.series);
        };
        return this;
    }-*/;

	public final native TooltipOptions setHeaderFormat(String headerFormat) /*-{
        this.headerFormat = headerFormat;
        return this;
    }-*/;

	public final native TooltipOptions setPointFormat(String pointFormat) /*-{
        this.pointFormat = pointFormat;
        return this;
    }-*/;

}