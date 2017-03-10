package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class SeriesOptions extends JavaScriptObject {
	public static SeriesOptions create() {
		return createObject().cast();
	}

	protected SeriesOptions() {
	}

	public final native SeriesOptions setPoint(PointOptions point) /*-{
        this.point = point;
        return this;
    }-*/;

	public final SeriesOptions setCursor(CursorType cursorType) {
		return setCursor(cursorType.getName());
	}

	protected final native SeriesOptions setCursor(String cursor) /*-{
        this.cursor = cursor;
        return this;
    }-*/;

	public final native SeriesOptions setEvents(SeriesEventOptions events) /*-{
        this.events = events;
        return this;
    }-*/;
}