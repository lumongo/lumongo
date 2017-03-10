package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class ButtonOptions extends JavaScriptObject {

	public static ButtonOptions create() {
		return createObject().cast();
	}

	protected ButtonOptions() {
	}

	public final ButtonOptions setVerticalAlign(VerticalAlignType verticalAlign) {
		return setVerticalAlign(verticalAlign.getName());
	}

	private final native ButtonOptions setVerticalAlign(String verticalAlign) /*-{
        this.verticalAlign = verticalAlign;
        return this;
    }-*/;

	public final ButtonOptions setAlign(AlignType align) {
		return setAlign(align.getName());
	}

	private final native ButtonOptions setAlign(String align) /*-{
        this.align = align;
        return this;
    }-*/;

}
