package org.lumongo.ui.client.places;

import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Document;
import com.google.gwt.dom.client.ScriptElement;
import com.google.gwt.place.shared.Place;
import com.google.gwt.place.shared.PlaceChangeEvent;
import com.google.gwt.place.shared.PlaceHistoryMapper;
import org.lumongo.ui.client.ContentPresenter;
import org.lumongo.ui.client.LumongoUI;
import org.lumongo.ui.client.controllers.MainController;
import org.lumongo.ui.client.controllers.WidgetController;
import org.lumongo.ui.client.eventbus.ResetSearchingEvent;

public class PlaceHandler implements PlaceChangeEvent.Handler {

	private Place lastPlace;
	private WidgetController widgetController;
	private final LumongoUI lumongoUI;

	public PlaceHandler() {
		lumongoUI = new LumongoUI();
		widgetController = new WidgetController();
	}

	public void init() {

		/*
		setGaVars("");
		initGA();
		*/

		MainController.get().getEventBus().addHandler(PlaceChangeEvent.TYPE, this);
		setupEvents();

		initPlaces();

	}

	protected void setupEvents() {

	}

	protected void initPlaces() {
		MainController.get().init(getPlaceHistoryMapper(), getDefaultPlace(), getHomePlace());
	}

	protected ContentPresenter getContentPresenter() {
		return lumongoUI;
	}

	protected WidgetController getWidgetController() {
		return widgetController;
	}

	protected PlaceHistoryMapper getPlaceHistoryMapper() {
		return new PlaceHistoryMapperImpl();
	}

	public Place getLastPlace() {
		return lastPlace;
	}

	public native void track(String url) /*-{
        try {
            //$wnd._gaq.push([ '_trackPageview' ]);
            $wnd._gaq.push(['_trackPageview', url]);
        } catch (e) {
        }
    }-*/;

	private void initGA() {
		Document doc = Document.get();
		ScriptElement script = doc.createScriptElement();
		script.setSrc("https://ssl.google-analytics.com/ga.js");
		script.setType("text/javascript");
		script.setLang("javascript");
		doc.getBody().appendChild(script);
	}

	private static native void setGaVars(String account) /*-{
        $wnd._gaq = $wnd._gaq || [];
        $wnd._gaq.push(['_setAccount', account]);
        $wnd._gaq.push(['_trackPageview',
            location.pathname + location.search + location.hash]);
    }-*/;

	@Override
	public void onPlaceChange(PlaceChangeEvent event) {

		final Place place = event.getNewPlace();
		track(place.getClass().getSimpleName());

		GWT.log(place.toString());

		lastPlace = place;

		setBrowserWindowTitle("LuMongo");

		handlePlaces(place);

	}

	protected final void handlePlaces(Place place) {
		if (place instanceof HomePlace) {
			displayHomePlace();
		}
		else if (place instanceof SearchPlace) {
			displaySearchPlace((SearchPlace) place);
		}
	}

	public boolean placeStillCurrent(Place place) {
		boolean current = (lastPlace == place);
		if (!current) {
			GWT.log("Place " + place + " is no longer current");
		}
		else {
			GWT.log("Place " + place + " is current");
		}
		return current;
	}

	public static void setBrowserWindowTitle(String newTitle) {
		if (Document.get() != null) {
			Document.get().setTitle(newTitle);
		}
	}

	protected final Place getDefaultPlace() {
		return getHomePlace();
	}

	protected Place getHomePlace() {
		return new HomePlace();
	}

	protected void displaySearchPlace(SearchPlace place) {

	}

	protected void displayHomePlace() {
		LumongoUI contentPresenter = (LumongoUI) getContentPresenter();
		getContentPresenter().setContent(null);

		// show the splash page...
		MainController.get().getEventBus().fireEvent(new ResetSearchingEvent());
		getWidgetController().getHomeView().drawSplashPage();
		getContentPresenter().setContent(getWidgetController().getHomeView());

	}

}
