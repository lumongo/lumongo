package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class ColumnOptions extends JavaScriptObject {

	public static ColumnOptions create() {
		return createObject().cast();
	}

	protected ColumnOptions() {
	}

	public final ColumnOptions setStacking(StackingType stackingType) {
		return setStacking(stackingType.getName());
	}

	protected final native ColumnOptions setStacking(String stacking) /*-{
        this.stacking = stacking;
        return this;
    }-*/;

	public final native ColumnOptions setMaxColumnWidth(int width) /*-{
        this.maxPointWidth = width;
        return this;
    }-*/;

	public final native ColumnOptions setPointOptions(PointOptions point) /*-{
        this.point = point;
        return this;
    }-*/;
}
