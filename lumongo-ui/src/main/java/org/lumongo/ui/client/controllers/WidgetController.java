package org.lumongo.ui.client.controllers;

import org.lumongo.ui.client.widgets.HomeView;
import org.lumongo.ui.client.widgets.query.QueryView;

/**
 * Created by Payam Meyer on 6/27/16.
 * @author pmeyer
 */
public class WidgetController {

	private HomeView homeView;
	private QueryView queryView;

	public WidgetController() {
		homeView = new HomeView();
		queryView = new QueryView();
	}

	public HomeView getHomeView() {
		return homeView;
	}

	public QueryView getQueryView() {
		return queryView;
	}
}
