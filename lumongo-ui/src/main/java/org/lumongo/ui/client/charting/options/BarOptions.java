package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class BarOptions extends JavaScriptObject {

	public static BarOptions create() {
		return createObject().cast();
	}

	protected BarOptions() {
	}

	public final BarOptions setStacking(StackingType stackingType) {
		return setStacking(stackingType.getName());
	}

	protected final native BarOptions setStacking(String stacking) /*-{
        this.stacking = stacking;
        return this;
    }-*/;

	public native final BarOptions setPoint(PointOptions point) /*-{
        this.point = point;
        return this;
    }-*/;

	public final native BarOptions setCursor(CursorType cursor) /*-{
        this.cursor = cursor;
        return this;
    }-*/;

}
