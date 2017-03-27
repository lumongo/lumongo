package org.lumongo.ui.client.widgets.base;

import gwt.material.design.client.ui.MaterialToast;

/**
 * Created by Payam Meyer on 3/13/17.
 * @author pmeyer
 */
public class ToastHelper {

	public static void showFailure(String message) {
		MaterialToast.fireToast(message, "red");
	}

	public static void showFailure(String message, Throwable caught) {
		MaterialToast.fireToast(message + " -- " + caught.getMessage(), "red");
	}

	public static void showSuccess(String message) {
		MaterialToast.fireToast(message, "green");
	}
}
