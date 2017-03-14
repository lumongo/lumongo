package org.lumongo.ui.client.charting;

import com.google.gwt.core.client.Callback;
import com.google.gwt.core.client.GWT;
import com.google.gwt.core.client.ScriptInjector;

/**
 * Created by Payam Meyer on 3/7/2016.
 * @author millm
 */
public abstract class HighChartsInjector {

	private boolean highchartsLoaded = false;
	private boolean jqueryLoaded = false;
	private boolean jqueryuiLoaded = false;

	public void inject() {
		// Highcharts
		ScriptInjector.fromUrl("https://code.highcharts.com/highcharts.js").setCallback(new Callback<Void, Exception>() {
			@Override
			public void onFailure(Exception reason) {
				GWT.log("Failed to load required library, Please file a ticket");
			}

			@Override
			public void onSuccess(Void result) {
				highchartsLoaded = true;
				GWT.log("Succeeded to load highcharts");
				ScriptInjector.fromUrl("https://code.highcharts.com/modules/exporting.js").setCallback(new Callback<Void, Exception>() {
					@Override
					public void onFailure(Exception reason) {
						GWT.log("Failed to load required library, Please file a ticket");
					}

					@Override
					public void onSuccess(Void result) {
						highchartsLoaded = true;
						onload();
						GWT.log("Succeeded to load highcharts exporting");
					}
				}).setWindow(ScriptInjector.TOP_WINDOW).inject();
			}
		}).setWindow(ScriptInjector.TOP_WINDOW).inject();

		/*
		//JQuery
		ScriptInjector.fromUrl("http://ajax.googleapis.com/ajax/libs/jquery/1.8.1/jquery.min.js").setCallback(new Callback<Void, Exception>() {
			@Override
			public void onFailure(Exception reason) {
				NotifyUtil.showFailure("Failed to load required library", "Please file a ticket");
			}

			@Override
			public void onSuccess(Void result) {
				GWT.log("Succeeded to load jquery");
				jqueryLoaded = true;
				ScriptInjector.fromUrl("http://ajax.googleapis.com/ajax/libs/jqueryui/1.8.23/jquery-ui.min.js").setCallback(new Callback<Void, Exception>() {
					@Override
					public void onFailure(Exception reason) {
						NotifyUtil.showFailure("Failed to load required library", "Please file a ticket");
					}

					@Override
					public void onSuccess(Void result) {
						jqueryuiLoaded = true;
						GWT.log("Succeeded to load jq ui");
					}
				}).setWindow(ScriptInjector.TOP_WINDOW).inject();
			}
		}).setWindow(ScriptInjector.TOP_WINDOW).inject();
		*/

	}

	public boolean isHighchartsLoaded() {
		return highchartsLoaded;
	}

	public boolean isJqueryLoaded() {
		return jqueryLoaded;
	}

	public boolean isJqueryuiLoaded() {
		return jqueryuiLoaded;
	}

	public abstract void onload();
}
