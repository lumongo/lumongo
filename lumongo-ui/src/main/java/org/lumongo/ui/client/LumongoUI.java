package org.lumongo.ui.client;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.Widget;
import gwt.material.design.client.ui.html.Div;
import gwt.material.design.client.ui.html.Main;
import org.lumongo.ui.client.bundle.MainResources;
import org.lumongo.ui.client.charting.HighChartsInjector;
import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.highlighter.HighlighterInjector;
import org.lumongo.ui.client.places.PlaceHandler;
import org.lumongo.ui.client.widgets.Footer;
import org.lumongo.ui.client.widgets.Header;

/**
 * Created by Payam Meyer on 4/10/16.
 * @author pmeyer
 */
public class LumongoUI implements ContentPresenter, EntryPoint {

	private Main simplePanel;
	private Footer footer;
	private Header header;

	@Override
	public void onModuleLoad() {

		MainResources.INSTANCE.mainGSS().ensureInjected();
		HighlighterInjector.inject();
		HighChartsInjector highChartsInjector = new HighChartsInjector() {
			@Override
			public void onload() {

				Div div = new Div();

				header = new Header();
				simplePanel = createBaseView();
				footer = new Footer();

				div.add(header);
				div.add(simplePanel);
				div.add(footer);

				PlaceHandler placeHandler = new PlaceHandler(LumongoUI.this);
				placeHandler.init();

				RootPanel.get().add(div);
			}
		};
		highChartsInjector.inject();

	}

	@Override
	public Main createBaseView() {

		simplePanel = new Main();
		simplePanel.setId("main-wrapper");
		simplePanel.setMarginTop(40);
		simplePanel.setMarginBottom(40);

		Highcharts.setExportUrl("");

		return simplePanel;
	}

	@Override
	public void setContent(Widget content) {
		simplePanel.clear();
		if (content != null) {
			simplePanel.add(content);
		}
	}

	public Header getHeader() {
		return header;
	}
}
