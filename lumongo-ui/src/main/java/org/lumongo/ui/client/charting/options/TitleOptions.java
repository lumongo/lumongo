package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class TitleOptions extends JavaScriptObject {

	public static TitleOptions create() {
		return createObject().cast();
	}

	protected TitleOptions() {
	}

	public final TitleOptions setAlign(AlignType alignType) {
		return setAlign(alignType.getName());
	}

	protected final native TitleOptions setAlign(String align) /*-{
        this.align = align;
        return this;
    }-*/;

	public final native TitleOptions setFloating(boolean floating) /*-{
        this.floating = floating;
        return this;
    }-*/;

	public final native TitleOptions setMargin(int margin) /*-{
        this.margin = margin;
        return this;
    }-*/;

	public final native TitleOptions setStyle(String style) /*-{
        this.style = style;
        return this;
    }-*/;

	public final native TitleOptions setText(String text) /*-{
        this.text = text;
        return this;
    }-*/;

	public final native String getText() /*-{
        return this.text;
    }-*/;

	public final native TitleOptions setUseHTML(boolean useHTML) /*-{
        this.useHTML = useHTML;
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