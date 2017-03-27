package org.lumongo.ui.client.widgets.base;

import gwt.material.design.client.constants.Color;
import gwt.material.design.client.constants.IconType;
import gwt.material.design.client.constants.NavBarType;
import gwt.material.design.client.constants.SideNavType;
import gwt.material.design.client.constants.TextAlign;
import gwt.material.design.client.ui.MaterialHeader;
import gwt.material.design.client.ui.MaterialImage;
import gwt.material.design.client.ui.MaterialLink;
import gwt.material.design.client.ui.MaterialNavBar;
import gwt.material.design.client.ui.MaterialNavBrand;
import gwt.material.design.client.ui.MaterialNavSection;
import gwt.material.design.client.ui.MaterialSideNav;
import org.lumongo.ui.client.bundle.MainResources;
import org.lumongo.ui.client.controllers.MainController;
import org.lumongo.ui.client.places.HomePlace;
import org.lumongo.ui.client.places.QueryPlace;

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
		sideNav.setWidth("60");
		sideNav.setId("sideNav");
		sideNav.setBackgroundColor(Color.GREY_LIGHTEN_3);
		sideNav.setCloseOnClick(true);
		sideNav.setType(SideNavType.PUSH);
		MaterialNavBrand navBrand = new MaterialNavBrand();
		navBrand.add(new MaterialImage(MainResources.INSTANCE.logo()));
		navBrand.setHref("#");
		navBrand.addClickHandler(clickEvent -> sideNav.hide());
		sideNav.add(navBrand);

		{
			MaterialLink overView = new MaterialLink(IconType.INFO);
			overView.setTitle("Overview");
			overView.addClickHandler(clickEvent -> MainController.get().goTo(new HomePlace()));
			sideNav.add(overView);
		}

		{
			MaterialLink indexes = new MaterialLink(IconType.SEARCH);
			indexes.setTitle("Query");
			indexes.addClickHandler(clickEvent -> MainController.get().goTo(new QueryPlace(null)));
			sideNav.add(indexes);
		}

		add(navBar);
		add(sideNav);

	}

	public MaterialSideNav getSideNav() {
		return sideNav;
	}

}
