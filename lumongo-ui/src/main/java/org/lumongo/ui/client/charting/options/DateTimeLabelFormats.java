package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class DateTimeLabelFormats extends JavaScriptObject {

	public static DateTimeLabelFormats create() {
		return createObject().cast();
	}

	protected DateTimeLabelFormats() {
	}

	public final native DateTimeLabelFormats setMillisecond(String millisecond) /*-{
        this.millisecond = millisecond;
        return this;
    }-*/;

	public final native DateTimeLabelFormats setSecond(String second) /*-{
        this.second = second;
        return this;
    }-*/;

	public final native DateTimeLabelFormats setMinute(String minute) /*-{
        this.minute = minute;
        return this;
    }-*/;

	public final native DateTimeLabelFormats setHour(String hour) /*-{
        this.hour = hour;
        return this;
    }-*/;

	public final native DateTimeLabelFormats setDay(String day) /*-{
        this.day = day;
        return this;
    }-*/;

	public final native DateTimeLabelFormats setWeek(String week) /*-{
        this.week = week;
        return this;
    }-*/;

	public final native DateTimeLabelFormats setMonth(String month) /*-{
        this.month = month;
        return this;
    }-*/;

	public final native DateTimeLabelFormats setYear(String year) /*-{
        this.year = year;
        return this;
    }-*/;
}
