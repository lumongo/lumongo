package org.lumongo.ui.client.widgets;

import com.google.gwt.dom.client.Style;
import com.google.gwt.user.client.Window;
import gwt.material.design.client.constants.Color;
import gwt.material.design.client.constants.FooterType;
import gwt.material.design.client.constants.IconType;
import gwt.material.design.client.ui.MaterialButton;
import gwt.material.design.client.ui.MaterialColumn;
import gwt.material.design.client.ui.MaterialFooter;
import gwt.material.design.client.ui.MaterialLabel;
import gwt.material.design.client.ui.MaterialRow;

/**
 * Created by Payam Meyer on 3/10/17.
 * @author pmeyer
 */
public class Footer extends MaterialFooter {

	public Footer() {

		setBackgroundColor(Color.GREY_DARKEN_2);

		MaterialRow row = new MaterialRow();
		MaterialColumn leftColumn = new MaterialColumn(12, 6, 6);
		MaterialColumn rightColumn = new MaterialColumn(12, 6, 6);
		row.add(leftColumn);
		row.add(rightColumn);
		add(row);

		setType(FooterType.FIXED);
		MaterialLabel label = new MaterialLabel("LuMongo is distributed under a commercially friendly Apache Software license");
		label.setTextColor(Color.WHITE);
		label.setMarginTop(15);
		leftColumn.add(label);

		MaterialButton chatButton = new MaterialButton("Chat with Us");
		chatButton.setMarginTop(10);
		chatButton.setMarginLeft(20);
		chatButton.setFloat(Style.Float.RIGHT);
		chatButton.setIconType(IconType.CHAT_BUBBLE);
		chatButton.addClickHandler(clickEvent -> Window
				.open("https://gitter.im/lumongo/lumongo?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge", "_blank",
						"menubar=1,status=1,toolbar=1,scrollbars=1,resizable=1"));
		rightColumn.add(chatButton);

		MaterialButton sourceButton = new MaterialButton("Source");
		sourceButton.setMarginTop(10);
		sourceButton.setIconType(IconType.CODE);
		sourceButton.setFloat(Style.Float.RIGHT);
		sourceButton.addClickHandler(
				clickEvent -> Window.open("https://github.com/lumongo/lumongo", "_blank", "menubar=1,status=1,toolbar=1,scrollbars=1,resizable=1"));
		rightColumn.add(sourceButton);

	}

}
