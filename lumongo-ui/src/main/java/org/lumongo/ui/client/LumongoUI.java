package org.lumongo.ui.client;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.SimplePanel;
import com.google.gwt.user.client.ui.Widget;
import gwt.material.design.client.ui.html.Div;
import gwt.material.design.client.ui.html.Main;
import org.lumongo.ui.client.bundle.MainResources;
import org.lumongo.ui.client.charting.HighChartsInjector;
import org.lumongo.ui.client.charting.Highcharts;
import org.lumongo.ui.client.highlighter.HighlighterInjector;
import org.lumongo.ui.client.places.PlaceHandler;
import org.lumongo.ui.client.widgets.base.Footer;
import org.lumongo.ui.client.widgets.base.Header;

/**
 * Created by Payam Meyer on 4/10/16.
 * @author pmeyer
 */
public class LumongoUI implements ContentPresenter, EntryPoint {

	private SimplePanel simplePanel;
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
				Main main = createBaseView();
				footer = new Footer();

				div.add(header);
				div.add(main);
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

		Main main = new Main();
		main.setId("main-wrapper");
		main.setMarginTop(40);
		main.setMarginBottom(40);

		simplePanel = new SimplePanel();
		main.add(simplePanel);

		Highcharts.setExportUrl("");

		return main;
	}

	@Override
	public void setContent(Widget content) {
		simplePanel.setWidget(content);
	}

	public Header getHeader() {
		return header;
	}
}
