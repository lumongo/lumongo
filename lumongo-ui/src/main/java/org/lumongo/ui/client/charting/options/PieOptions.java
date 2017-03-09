package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

/**
 * Created by millm on 3/27/2016.
 */
public class PieOptions extends JavaScriptObject {
	public static PieOptions create() {
		return createObject().cast();
	}

	protected PieOptions() {
	}

	public native final PieOptions setCursor(CursorType cursor) /*-{
        this.cursor = cursor;
        return this;
    }-*/;

	public native final PieOptions setPoint(PointOptions point) /*-{
        this.point = point;
        return this;
    }-*/;
}
