package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;
import org.lumongo.ui.client.charting.handlers.ChartDataSelectionHandler;

/**
 * Created by millm on 4/12/2016.
 */
public class ColumnEventOptions extends JavaScriptObject {
	public static ColumnEventOptions create() {
		return createObject().cast();
	}

	protected ColumnEventOptions() {
	}

	public final native ColumnEventOptions setClick(ChartDataSelectionHandler handler) /*-{
        this.click = function () {
            return handler.@org.lumongo.ui.client.charting.handlers.ChartDataSelectionHandler::onClick(Ljava/lang/String;Ljava/lang/Integer;)(this.category, this.y);
        };
        return this;
    }-*/;
}
