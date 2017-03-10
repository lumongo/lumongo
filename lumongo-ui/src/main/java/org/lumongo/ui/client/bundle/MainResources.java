package org.lumongo.ui.client.bundle;

import com.google.gwt.core.client.GWT;
import com.google.gwt.resources.client.ClientBundle;
import com.google.gwt.resources.client.ImageResource;

/**
 * Created by Matt Davis on 4/13/16.
 * @author mdavis
 */
public interface MainResources extends ClientBundle {

	MainResources INSTANCE = GWT.create(MainResources.class);

	MainGSS GSS = INSTANCE.mainGSS();

	@ClientBundle.Source("main.gss")
	MainGSS mainGSS();

	@Source("images/lumongoLogo.png")
	ImageResource logo();

	@Source("images/lumongoLogoLong.png")
	ImageResource logoLong();

}
