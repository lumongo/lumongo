package org.lumongo.ui.client;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.user.client.ui.HTMLPanel;
import com.google.gwt.user.client.ui.RootPanel;
import com.vaadin.polymer.Polymer;
import com.vaadin.polymer.paper.widget.PaperDrawerPanel;
import com.vaadin.polymer.paper.widget.PaperHeaderPanel;
import com.vaadin.polymer.paper.widget.PaperIconButton;
import com.vaadin.polymer.paper.widget.PaperItem;
import com.vaadin.polymer.paper.widget.PaperMaterial;
import com.vaadin.polymer.paper.widget.PaperMenu;
import com.vaadin.polymer.paper.widget.PaperToolbar;

import java.util.Arrays;

/**
 * Created by mdavis on 4/10/16.
 */
public class LumongoUI implements EntryPoint {
	@Override
	public void onModuleLoad() {

		Polymer.importHref(Arrays.asList("iron-icons/iron-icons.html", "iron-flex-layout/iron-flex-layout.html"));

		PaperDrawerPanel paperDrawerPanel = new PaperDrawerPanel();

		PaperHeaderPanel drawerHeaderPanel = getDrawerPanel();

		paperDrawerPanel.add(drawerHeaderPanel);

		PaperHeaderPanel mainHeaderPanel = getMainPanel();

		paperDrawerPanel.add(mainHeaderPanel);

		RootPanel.get().add(paperDrawerPanel);
	}

	private PaperHeaderPanel getDrawerPanel() {
		PaperHeaderPanel drawerHeaderPanel = new PaperHeaderPanel();
		drawerHeaderPanel.setAttributes("drawer");

		PaperToolbar toolbar = new PaperToolbar();
		toolbar.add(new PaperItem("LuMongo"));
		drawerHeaderPanel.add(toolbar);

		PaperMenu paperMenu = new PaperMenu();

		paperMenu.add(new PaperMaterial("Search"));
		paperMenu.add(new PaperMaterial("Admin"));
		paperMenu.add(new PaperMaterial("Something"));

		drawerHeaderPanel.add(paperMenu);

		return drawerHeaderPanel;
	}

	private PaperHeaderPanel getMainPanel() {
		PaperHeaderPanel drawerHeaderPanel = new PaperHeaderPanel();
		drawerHeaderPanel.setAttributes("main");

		PaperToolbar toolbar = new PaperToolbar();

		HTMLPanel headerWrapper = new HTMLPanel("");
		headerWrapper.addStyleName("layout");
		headerWrapper.addStyleName("horizontal");
		headerWrapper.addStyleName("center");
		PaperIconButton menu = new PaperIconButton();
		menu.setIcon("menu");
		menu.setAttributes("paper-drawer-toggle");
		headerWrapper.add(menu);
		HTMLPanel main = new HTMLPanel("Main");
		main.addStyleName("flex");
		headerWrapper.add(main);

		PaperIconButton apps = new PaperIconButton();
		apps.setIcon("apps");
		headerWrapper.add(apps);

		toolbar.add(headerWrapper);

		drawerHeaderPanel.add(toolbar);

		return drawerHeaderPanel;
	}
}
