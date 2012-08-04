package org.lumongo.somongo.client.css;

import com.google.gwt.core.client.GWT;
import com.google.gwt.resources.client.ClientBundle;

public interface DefaultClientBundle extends ClientBundle {
	public static final DefaultClientBundle INSTANCE = GWT.create(DefaultClientBundle.class);
	
	@Source("default.css")
	DefaultCSS css();
}
