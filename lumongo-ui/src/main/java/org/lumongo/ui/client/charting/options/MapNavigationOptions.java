package org.lumongo.ui.client.charting.options;

import com.google.gwt.core.client.JavaScriptObject;

public class MapNavigationOptions extends JavaScriptObject {

	public static MapNavigationOptions create() {
		return createObject().cast();
	}

	protected MapNavigationOptions() {
	}

	public final native MapNavigationOptions setEnabled(boolean enabled) /*-{
        this.enabled = enabled;
        return this;
    }-*/;

	public final native MapNavigationOptions setEnableTouchZoom(boolean enableTouchZoom) /*-{
        this.enableTouchZoom = enableTouchZoom;
        return this;
    }-*/;

	public final native MapNavigationOptions setEnableMouseWheelZoom(boolean enableMouseWheelZoom) /*-{
        this.enableMouseWheelZoom = enableMouseWheelZoom;
        return this;
    }-*/;

	public final native MapNavigationOptions setEnableDoubleClickZoomTo(boolean enableDoubleClickZoomTo) /*-{
        this.enableDoubleClickZoomTo = enableDoubleClickZoomTo;
        return this;
    }-*/;

	public final native MapNavigationOptions setEnableDoubleClickZoom(boolean enableDoubleClickZoom) /*-{
        this.enableDoubleClickZoom = enableDoubleClickZoom;
        return this;
    }-*/;

	public final native MapNavigationOptions setEnableButtons(boolean enableButtons) /*-{
        this.enableButtons = enableButtons;
        return this;
    }-*/;

	public final native MapNavigationOptions setButtonOptions(ButtonOptions buttonOptions) /*-{
        this.buttonOptions = buttonOptions;
        return this;
    }-*/;
}
