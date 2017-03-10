package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;
import org.lumongo.ui.client.charting.handlers.ChartDataSelectionHandler;

/**
 * Created by Payam Meyer on 4/8/2016.
 * @author pmeyer
 */
public class SeriesEventOptions extends JavaScriptObject {
	public static SeriesEventOptions create() {
		return createObject().cast();
	}

	protected SeriesEventOptions() {
	}

	public final native SeriesEventOptions setClick(ChartDataSelectionHandler handler) /*-{
        this.click = function () {
            return handler.@org.lumongo.ui.client.charting.handlers.ChartDataSelectionHandler::onClick(Ljava/lang/String;Ljava/lang/Integer;)(this.category, this.y);
        };
        return this;
    }-*/;
}
