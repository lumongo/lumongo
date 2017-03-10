package org.lumongo.ui.client;

import com.google.gwt.user.client.ui.Widget;
import gwt.material.design.client.ui.html.Main;

/**
 * Created by Payam Meyer on 3/9/17.
 * @author pmeyer
 */
public interface ContentPresenter {

	Main createBaseView();

	void setContent(Widget content);

}
