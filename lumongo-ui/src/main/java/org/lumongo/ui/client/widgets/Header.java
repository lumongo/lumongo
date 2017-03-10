package org.lumongo.ui.client.widgets;

import gwt.material.design.client.constants.Color;
import gwt.material.design.client.constants.NavBarType;
import gwt.material.design.client.constants.SideNavType;
import gwt.material.design.client.constants.TextAlign;
import gwt.material.design.client.ui.MaterialHeader;
import gwt.material.design.client.ui.MaterialNavBar;
import gwt.material.design.client.ui.MaterialNavBrand;
import gwt.material.design.client.ui.MaterialNavSection;
import gwt.material.design.client.ui.MaterialSideNav;

/**
 * Created by Payam Meyer on 3/10/17.
 * @author pmeyer
 */
public class Header extends MaterialHeader {

	private final MaterialSideNav sideNav;

	public Header() {

		MaterialNavBar navBar = new MaterialNavBar();
		navBar.setActivates("sideNav");
		navBar.setType(NavBarType.FIXED);
		navBar.setWidth("100%");
		navBar.setBackgroundColor(Color.GREY_DARKEN_2);

		MaterialNavSection navSection = new MaterialNavSection();
		navSection.setTextAlign(TextAlign.RIGHT);

		navBar.add(navSection);

		sideNav = new MaterialSideNav(SideNavType.PUSH);
		sideNav.setWidth("200");
		sideNav.setId("sideNav");
		sideNav.setBackgroundColor(Color.GREY_LIGHTEN_3);
		sideNav.setCloseOnClick(false);
		sideNav.hide();
		MaterialNavBrand navBrand = new MaterialNavBrand("LuMongo");
		navBrand.setHref("#");
		navBrand.addClickHandler(clickEvent -> sideNav.hide());
		sideNav.add(navBrand);

		add(navBar);
		add(sideNav);

	}

	public MaterialSideNav getSideNav() {
		return sideNav;
	}
}
