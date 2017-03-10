package org.lumongo.ui.client;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.SimplePanel;
import com.google.gwt.user.client.ui.Widget;
import gwt.material.design.client.ui.MaterialHeader;
import gwt.material.design.client.ui.html.Div;
import gwt.material.design.client.ui.html.Main;
import org.lumongo.ui.client.bundle.MainResources;
import org.lumongo.ui.client.charting.HighChartsInjector;
import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.places.PlaceHandler;
import org.lumongo.ui.client.widgets.Footer;
import org.lumongo.ui.client.widgets.Header;

/**
 * Created by Payam Meyer on 4/10/16.
 * @author pmeyer
 */
public class LumongoUI implements ContentPresenter, EntryPoint {

	private SimplePanel simplePanel;
	private Widget footer;
	private MaterialHeader header;

	@Override
	public void onModuleLoad() {

		MainResources.INSTANCE.mainGSS().ensureInjected();
		new HighChartsInjector().inject();

		Div div = new Div();

		header = new Header();
		Main baseView = createBaseView();
		footer = new Footer();

		div.add(header);
		div.add(baseView);
		div.add(footer);

		PlaceHandler placeHandler = new PlaceHandler(this);
		placeHandler.init();

		RootPanel.get().add(div);

	}

	@Override
	public Main createBaseView() {
		MainResources.GSS.ensureInjected();

		final Main mainWrapper = new Main();
		mainWrapper.setId("main-wrapper");

		Highcharts.setExportUrl("");

		simplePanel = new SimplePanel();
		simplePanel.getElement().setId("contentContainer");

		mainWrapper.add(simplePanel);

		return mainWrapper;
	}

	@Override
	public void setContent(Widget content) {
		simplePanel.setWidget(content);
	}

	public MaterialHeader getHeader() {
		return header;
	}
}
