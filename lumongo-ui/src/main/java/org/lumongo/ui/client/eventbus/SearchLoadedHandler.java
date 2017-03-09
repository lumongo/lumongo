package org.lumongo.ui.client.eventbus;

import com.google.gwt.event.shared.EventHandler;
import org.lumongo.ui.shared.UIQueryResults;

/**
 * Created by Payam Meyer on 5/20/15.
 * @author pmeyer
 */
public interface SearchLoadedHandler extends EventHandler {

	void handleSearchLoaded(UIQueryResults uiQueryResults);
}
