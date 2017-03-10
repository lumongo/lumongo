package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class LegendOptions extends JavaScriptObject {

	public static LegendOptions create() {
		return createObject().cast();
	}

	protected LegendOptions() {
	}

	public final LegendOptions setAlign(AlignType alignType) {
		return setAlign(alignType.getName());
	}

	public final native LegendOptions setAlign(String align) /*-{
        this.align = align;
        return this;
    }-*/;

	public final native TitleOptions setFloating(boolean floating) /*-{
        this.floating = floating;
        return this;
    }-*/;

	public final native TitleOptions setShadow(boolean shadow) /*-{
        this.shadow = shadow;
        return this;
    }-*/;

	public final native TitleOptions setBorderWidth(int borderWidth) /*-{
        this.borderWidth = borderWidth;
        return this;
    }-*/;

	public final native TitleOptions setBorderColor(String borderColor) /*-{
        this.borderColor = borderColor;
        return this;
    }-*/;

	public final TitleOptions setVerticalAlign(VerticalAlignType verticalAlignType) {
		return setVerticalAlign(verticalAlignType.getName());
	}

	protected final native TitleOptions setVerticalAlign(String verticalAlign) /*-{
        this.verticalAlign = verticalAlign;
        return this;
    }-*/;

	public final native TitleOptions setX(int x) /*-{
        this.x = x;
        return this;
    }-*/;

	public final native TitleOptions setY(int y) /*-{
        this.y = y;
        return this;
    }-*/;

}