package org.lumongo.somongo.client;

import org.lumongo.somongo.client.css.DefaultClientBundle;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.dom.client.Style.Unit;
import com.google.gwt.user.client.ui.DockLayoutPanel;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RootLayoutPanel;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

public class SoMongo implements EntryPoint {

    // private final PersistServiceAsync persistService =
    // GWT.create(PersistService.class);

    @Override
    public void onModuleLoad() {
        DefaultClientBundle.INSTANCE.css().ensureInjected();

        DockLayoutPanel main = new DockLayoutPanel(Unit.PX);

        main.addNorth(createHeader(), 100);
        main.addWest(createNavigation(), 150);
        main.add(createMain());

        RootLayoutPanel.get().add(main);

    }

    protected Widget createHeader() {

        HorizontalPanel header = new HorizontalPanel();
        header.add(new Label("header"));
        return header;
    }

    protected Widget createNavigation() {
        VerticalPanel nav = new VerticalPanel();
        nav.add(new Label("nav"));
        return nav;
    }

    protected Widget createMain() {
        VerticalPanel main = new VerticalPanel();
        main.add(new Label("nav"));
        return main;

    }

}
