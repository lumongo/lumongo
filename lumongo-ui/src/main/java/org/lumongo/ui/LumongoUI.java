package org.lumongo.ui;

import com.vaadin.server.VaadinRequest;
import com.vaadin.ui.Button;
import com.vaadin.ui.Grid;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.UI;
import com.vaadin.ui.Label;
import com.vaadin.annotations.Theme;
import com.vaadin.ui.VerticalLayout;

@Theme("Lumongo")
public class LumongoUI extends UI{
	
	@Override
	protected void init(VaadinRequest request){
		HorizontalLayout main = new HorizontalLayout();

		VerticalLayout menu = new VerticalLayout();
		menu.setWidth(200, Unit.PIXELS);
		menu.addComponent(new Label("Search"));
		menu.addComponent(new Label("Admin"));
		menu.addComponent(new Label("Stuff"));

		VerticalLayout content = new VerticalLayout();

		content.addComponent(new Button("Do Something"));


		main.addComponent(menu);
		main.addComponent(content);

		setContent(main);
	}
}
