package org.lumongo.ui.client;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.SimplePanel;
import com.google.gwt.user.client.ui.Widget;
import gwt.material.design.client.ui.MaterialHeader;
import gwt.material.design.client.ui.MaterialSection;
import gwt.material.design.client.ui.html.Main;
import org.lumongo.ui.client.bundle.MainResources;
import org.lumongo.ui.client.charting.HighChartsInjector;
import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.places.PlaceHandler;

/**
 * Created by Payam Meyer on 4/10/16.
 * @author pmeyer
 */
public class LumongoUI implements ContentPresenter, EntryPoint {

	private SimplePanel simplePanel;
	private Widget footer;
	private MaterialHeader header;
	private MaterialSection mainContentWrapper;

	@Override
	public void onModuleLoad() {

		MainResources.INSTANCE.mainGSS().ensureInjected();
		new HighChartsInjector().inject();

		Main baseView = createBaseView();

		PlaceHandler placeHandler = new PlaceHandler();
		placeHandler.init();

		RootPanel.get().add(baseView);

	}

	@Override
	public Main createBaseView() {
		MainResources.GSS.ensureInjected();

		final Main mainWrapper = new Main();
		mainWrapper.setId("main-wrapper");

		Highcharts.setExportUrl("");

		mainContentWrapper = new MaterialSection();
		mainContentWrapper.addStyleName(MainResources.GSS.materialContent());

		simplePanel = new SimplePanel();
		simplePanel.getElement().setId("contentContainer");

		mainContentWrapper.add(simplePanel);

		mainWrapper.add(header);
		mainWrapper.add(mainContentWrapper);
		mainWrapper.add(footer);

		return mainWrapper;
	}

	@Override
	public void setContent(Widget content) {
		simplePanel.setWidget(content);
	}
}
