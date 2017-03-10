package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class SubtitleOptions extends JavaScriptObject {

	public static SubtitleOptions create() {
		return createObject().cast();
	}

	protected SubtitleOptions() {
	}

	public final SubtitleOptions setAlign(AlignType alignType) {
		return setAlign(alignType.getName());
	}

	protected final native SubtitleOptions setAlign(String align) /*-{
        this.align = align;
        return this;
    }-*/;

	public final native SubtitleOptions setFloating(boolean floating) /*-{
        this.floating = floating;
        return this;
    }-*/;

	public final native SubtitleOptions setStyle(String style) /*-{
        this.style = style;
        return this;
    }-*/;

	public final native SubtitleOptions setText(String text) /*-{
        this.text = text;
        return this;
    }-*/;

	public final native SubtitleOptions setUseHTML(boolean useHTML) /*-{
        this.useHTML = useHTML;
        return this;
    }-*/;

	public final SubtitleOptions setVerticalAlign(VerticalAlignType verticalAlignType) {
		return setAlign(verticalAlignType.getName());
	}

	protected final native SubtitleOptions setVerticalAlign(String verticalAlign) /*-{
        this.verticalAlign = verticalAlign;
        return this;
    }-*/;

	public final native SubtitleOptions setX(int x) /*-{
        this.x = x;
        return this;
    }-*/;

	public final native SubtitleOptions setY(int y) /*-{
        this.y = y;
        return this;
    }-*/;

}