package org.lumongo.ui.client.services;

import com.google.gwt.core.shared.GWT;

/**
 * Created by Payam Meyer on 5/13/15.
 * @author pmeyer
 */
public class ServiceProvider {

	private static ServiceProvider serviceProvider = new ServiceProvider();

	public static ServiceProvider get() {
		return serviceProvider;
	}

	private UIQueryServiceAsync searchService;

	private ServiceProvider() {
		searchService = GWT.create(UIQueryService.class);
	}

	public UIQueryServiceAsync getSearchService() {
		return searchService;
	}

}
