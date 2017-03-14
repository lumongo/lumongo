package org.lumongo.ui.client.widgets;

import gwt.material.design.client.constants.Color;
import gwt.material.design.client.constants.IconType;
import gwt.material.design.client.constants.NavBarType;
import gwt.material.design.client.constants.SideNavType;
import gwt.material.design.client.constants.TextAlign;
import gwt.material.design.client.ui.MaterialCollapsible;
import gwt.material.design.client.ui.MaterialCollapsibleBody;
import gwt.material.design.client.ui.MaterialCollapsibleHeader;
import gwt.material.design.client.ui.MaterialCollapsibleItem;
import gwt.material.design.client.ui.MaterialHeader;
import gwt.material.design.client.ui.MaterialIcon;
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
import org.lumongo.ui.shared.IndexInfo;
import org.lumongo.ui.shared.InstanceInfo;

/**
 * Created by Payam Meyer on 3/10/17.
 * @author pmeyer
 */
public class Header extends MaterialHeader {

	private final MaterialSideNav sideNav;
	private final MaterialCollapsibleBody materialCollapsibleBody;

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
		MaterialNavBrand navBrand = new MaterialNavBrand();
		navBrand.add(new MaterialImage(MainResources.INSTANCE.logoLong()));
		navBrand.setHref("#");
		navBrand.addClickHandler(clickEvent -> sideNav.hide());
		sideNav.add(navBrand);

		MaterialCollapsible collapsible = new MaterialCollapsible();
		MaterialCollapsibleItem materialCollapsibleItem = new MaterialCollapsibleItem();
		MaterialCollapsibleHeader collapsibleHeader = new MaterialCollapsibleHeader();
		materialCollapsibleBody = new MaterialCollapsibleBody();
		materialCollapsibleItem.add(collapsibleHeader);
		materialCollapsibleItem.add(materialCollapsibleBody);
		collapsible.add(materialCollapsibleItem);
		MaterialLink overView = new MaterialLink("Overview", new MaterialIcon(IconType.INFO));
		overView.addClickHandler(clickEvent -> MainController.get().goTo(new HomePlace()));
		MaterialLink indexes = new MaterialLink("Indexes", new MaterialIcon(IconType.SEARCH));
		collapsibleHeader.add(indexes);
		sideNav.add(overView);
		sideNav.add(collapsible);

		add(navBar);
		add(sideNav);

	}

	public MaterialSideNav getSideNav() {
		return sideNav;
	}

	public void setSideNavItems(InstanceInfo sideNavItems) {
		materialCollapsibleBody.clear();

		for (IndexInfo indexInfo : sideNavItems.getIndexes()) {
			MaterialLink indexLink = new MaterialLink(indexInfo.getName());
			indexLink.addClickHandler(clickEvent -> MainController.get().goTo(new QueryPlace(indexInfo.getName(), null)));
			materialCollapsibleBody.add(indexLink);
		}
	}
}
