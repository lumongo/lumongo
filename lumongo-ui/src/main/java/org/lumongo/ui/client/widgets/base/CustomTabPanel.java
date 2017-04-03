package org.lumongo.ui.client.widgets.base;

import com.google.gwt.user.client.ui.Widget;
import gwt.material.design.client.constants.Color;
import gwt.material.design.client.constants.IconType;
import gwt.material.design.client.constants.TabType;
import gwt.material.design.client.constants.WavesType;
import gwt.material.design.client.ui.MaterialColumn;
import gwt.material.design.client.ui.MaterialLink;
import gwt.material.design.client.ui.MaterialRow;
import gwt.material.design.client.ui.MaterialTab;
import gwt.material.design.client.ui.MaterialTabItem;

/**
 * Created by Payam Meyer on 9/16/16.
 * @author pmeyer
 */
public class CustomTabPanel extends MaterialRow {

	private MaterialTab materialTab;
	private final MaterialColumn mainColumn;

	public CustomTabPanel(TabType tabType) {

		materialTab = new MaterialTab();
		materialTab.setType(tabType);
		materialTab.setBackgroundColor(Color.GREY_DARKEN_1);
		materialTab.setTextColor(Color.GREY_DARKEN_1);
		materialTab.setIndicatorColor(Color.AMBER);

		mainColumn = new MaterialColumn();
		mainColumn.setGrid("s12");
		mainColumn.add(materialTab);

		add(mainColumn);

	}

	public MaterialTabItem createAndAddTabListItem(IconType iconType, String title, String dataTarget) {
		MaterialTabItem tabItem = new MaterialTabItem();
		tabItem.setWaves(WavesType.YELLOW);
		tabItem.setGrid("s4");

		MaterialLink materialLink = new MaterialLink(iconType);
		materialLink.setHref(dataTarget);
		materialLink.setText(title);
		materialLink.setTextColor(Color.WHITE);
		tabItem.add(materialLink);

		materialTab.add(tabItem);

		return tabItem;
	}

	public MaterialTabItem createAndAddTabListItem(String tabName, String dataTarget) {
		MaterialTabItem tabItem = new MaterialTabItem();
		tabItem.setGrid("s4");
		tabItem.setWaves(WavesType.YELLOW);

		MaterialLink materialLink = new MaterialLink(tabName);
		materialLink.setHref(dataTarget);
		tabItem.add(materialLink);

		materialTab.add(tabItem);

		return tabItem;
	}

	public void createAndAddTabPane(String id, Widget widget) {
		MaterialColumn materialColumn = new MaterialColumn();
		materialColumn.setId(id);
		materialColumn.setGrid("s12");
		materialColumn.add(widget);
		materialColumn.setBackgroundColor(Color.WHITE);

		mainColumn.add(materialColumn);

	}

}