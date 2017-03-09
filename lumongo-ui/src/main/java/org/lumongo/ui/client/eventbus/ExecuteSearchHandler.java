package org.lumongo.ui.client.eventbus;

import com.google.gwt.event.shared.EventHandler;
import org.lumongo.ui.shared.UIQueryObject;

/**
 * Created by Payam Meyer on 5/20/15.
 * @author pmeyer
 */
public interface ExecuteSearchHandler extends EventHandler {

	void handleExecuteSearch(UIQueryObject search);
}
