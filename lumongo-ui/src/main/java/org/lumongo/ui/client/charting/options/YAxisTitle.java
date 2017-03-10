package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class YAxisTitle extends JavaScriptObject {

	public static YAxisTitle create() {
		return createObject().cast();
	}

	protected YAxisTitle() {
	}

	public final YAxisTitle setAlign(AxisAlignType axisAlignType) {
		return setAlign(axisAlignType.getName());
	}

	protected final native YAxisTitle setAlign(String align) /*-{
        this.align = align;
        return this;
    }-*/;

	public final native YAxisTitle setMargin(int margin) /*-{
        this.margin = margin;
        return this;
    }-*/;

	public final native YAxisTitle setOffset(int offset) /*-{
        this.offset = offset;
        return this;
    }-*/;

	public final native YAxisTitle setRotation(int rotation) /*-{
        this.rotation = rotation;
        return this;
    }-*/;

	public final native YAxisTitle setStyle(String style) /*-{
        this.style = style;
        return this;
    }-*/;

	public final native YAxisTitle setText(String text) /*-{
        this.text = text;
        return this;
    }-*/;

}