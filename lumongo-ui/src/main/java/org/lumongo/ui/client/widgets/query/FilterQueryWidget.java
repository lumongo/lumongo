package org.lumongo.ui.client.widgets.query;

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
public class FilterQueryWidget extends Div {

	private final TextBox textBox;
	private MaterialLink plusButton;
	private MaterialLink minusButton;

	public FilterQueryWidget() {
		addStyleName(MainResources.GSS.searchBox());

		textBox = new TextBox();
		textBox.setStyleName(MainResources.GSS.searchBoxInput());

		add(textBox);

		plusButton = new MaterialLink();
		plusButton.setIconType(IconType.ADD);
		plusButton.setIconColor(Color.WHITE);
		add(plusButton);

		minusButton = new MaterialLink();
		minusButton.setIconType(IconType.REMOVE);
		minusButton.setIconColor(Color.WHITE);

		add(minusButton);

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

	public MaterialLink getPlusButton() {
		return plusButton;
	}

	public MaterialLink getMinusButton() {
		return minusButton;
	}

	public String getValue() {
		return textBox.getValue();
	}
}
