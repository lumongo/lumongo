package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class XAxisTitle extends JavaScriptObject {

	public static XAxisTitle create() {
		return createObject().cast();
	}

	protected XAxisTitle() {
	}

	public final XAxisTitle setAlign(AxisAlignType axisAlignType) {
		return setAlign(axisAlignType.getName());
	}

	protected final native XAxisTitle setAlign(String align) /*-{
        this.align = align;
        return this;
    }-*/;

	public final native XAxisTitle setMargin(int margin) /*-{
        this.margin = margin;
        return this;
    }-*/;

	public final native XAxisTitle setOffset(int offset) /*-{
        this.offset = offset;
        return this;
    }-*/;

	public final native XAxisTitle setRotation(int rotation) /*-{
        this.rotation = rotation;
        return this;
    }-*/;

	public final native XAxisTitle setStyle(String style) /*-{
        this.style = style;
        return this;
    }-*/;

	public final native XAxisTitle setText(String text) /*-{
        this.text = text;
        return this;
    }-*/;

}