package org.lumongo.ui.client;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.dom.client.StyleElement;
import com.google.gwt.dom.client.StyleInjector;
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
import org.lumongo.ui.client.bundle.MainResources;

import java.util.Arrays;

/**
 * Created by mdavis on 4/10/16.
 */
public class LumongoUI implements EntryPoint {
	@Override
	public void onModuleLoad() {

		MainResources.INSTANCE.mainGSS().ensureInjected();

		Polymer.importHref(Arrays.asList("iron-icons/iron-icons.html", "iron-flex-layout/classes/iron-flex-layout.html"), o -> {
			loadPage();

			return null;
		});


	}

	private void loadPage() {

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
		headerWrapper.setWidth("100%");

		PaperIconButton menu = new PaperIconButton();
		menu.setIcon("menu");
		menu.setAttributes("paper-drawer-toggle");
		headerWrapper.add(menu);

		HTMLPanel main = new HTMLPanel("Search");
		main.addStyleName("flex");
		headerWrapper.add(main);

		PaperIconButton apps = new PaperIconButton();
		apps.setIcon("apps");
		headerWrapper.add(apps);

		toolbar.add(headerWrapper);

		drawerHeaderPanel.add(toolbar);

		HTMLPanel content = new HTMLPanel("");
		content.addStyleName(MainResources.INSTANCE.mainGSS().card());
		PaperMaterial card1 = new PaperMaterial();
		card1.setElevation(2);
		PaperItem item1 = new PaperItem("Item 1");
		card1.add(item1);

		item1.setHeight("200px");
		content.add(card1);


		PaperMaterial card2 = new PaperMaterial();
		card2.setElevation(2);
		PaperItem item2= new PaperItem("Item 2");
		item2.setHeight("200px");
		card2.add(item2);
		content.add(card2);

		drawerHeaderPanel.add(content);

		return drawerHeaderPanel;
	}
}
