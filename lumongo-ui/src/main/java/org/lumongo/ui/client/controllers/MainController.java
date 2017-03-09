package org.lumongo.ui.client.controllers;

import com.google.gwt.event.shared.SimpleEventBus;
import com.google.gwt.place.shared.Place;
import com.google.gwt.place.shared.PlaceController;
import com.google.gwt.place.shared.PlaceHistoryHandler;
import com.google.gwt.place.shared.PlaceHistoryMapper;
import com.google.web.bindery.event.shared.EventBus;

public class MainController {

	private static MainController mainController = new MainController();

	public static MainController get() {
		return mainController;
	}

	private EventBus eventBus;

	private PlaceController placeController;

	private PlaceHistoryMapper placeHistoryMapper;

	private Place defaultPlace;
	private Place homePlace;

	private MainController() {
		eventBus = new SimpleEventBus();
		placeController = new PlaceController(eventBus);
	}

	public void init(PlaceHistoryMapper placeHistoryMapper, Place defaultPlace, Place homePlace) {
		this.placeHistoryMapper = placeHistoryMapper;
		this.defaultPlace = defaultPlace;
		this.homePlace = homePlace;
		PlaceHistoryHandler historyHandler = new PlaceHistoryHandler(placeHistoryMapper);
		historyHandler.register(placeController, eventBus, defaultPlace);
		historyHandler.handleCurrentHistory();
	}

	public EventBus getEventBus() {
		return eventBus;
	}

	public PlaceController getPlaceController() {
		return placeController;
	}

	public void goTo(Place place) {
		placeController.goTo(place);
	}

	public String getLinkToPlace(Place place) {
		return "#" + placeHistoryMapper.getToken(place);
	}

	public Place placeFromLink(String link) {
		return placeHistoryMapper.getPlace(link);
	}

	public Place getDefaultPlace() {
		return defaultPlace;
	}

	public Place getHomePlace() {
		return homePlace;
	}

	public Place getCurrentPlace() {
		return placeController.getWhere();
	}

}