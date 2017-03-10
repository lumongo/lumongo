package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;
import org.lumongo.ui.client.charting.handlers.ChartDataSelectionHandler;

public class PieEventOptions extends JavaScriptObject {

	public static PieEventOptions create() {
		return createObject().cast();
	}

	protected PieEventOptions() {
	}

	public final native PieEventOptions setClick(ChartDataSelectionHandler click) /*-{
        this.click = function () {
            return click.@org.lumongo.ui.client.charting.handlers.ChartDataSelectionHandler::onClick(Ljava/lang/String;Ljava/lang/Integer;)(this.name, this.y);
        };
        return this;
    }-*/;
}