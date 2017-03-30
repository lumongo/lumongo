package org.lumongo.ui.client.widgets.base;

import com.google.gwt.user.client.ui.TextBox;
import gwt.material.design.client.constants.Color;
import gwt.material.design.client.constants.IconType;
import gwt.material.design.client.ui.MaterialLink;
import gwt.material.design.client.ui.html.Div;
import org.lumongo.ui.client.bundle.MainResources;

/**
 * Created by Payam Meyer on 3/30/17.
 * @author pmeyer
 */
public class CustomTextBox extends Div {

	private final TextBox textBox;
	private MaterialLink button;

	public CustomTextBox() {
		this(false);
	}

	public CustomTextBox(boolean hasButton) {
		addStyleName(MainResources.GSS.searchBox());

		textBox = new TextBox();
		textBox.setStyleName(MainResources.GSS.searchBoxInput());

		add(textBox);

		if (hasButton) {
			button = new MaterialLink();
			button.setIconType(IconType.SEARCH);
			button.setIconColor(Color.WHITE);
			add(button);
		}

	}

	public void setPlaceHolder(String text) {
		textBox.getElement().setAttribute("placeholder", text);
	}

	public void setValue(String text) {
		textBox.setValue(text);
	}

	public TextBox getTextBox() {
		return textBox;
	}

	public MaterialLink getButton() {
		return button;
	}

	public String getValue() {
		return textBox.getValue();
	}
}
