package org.lumongo.ui.client.eventbus;

import com.google.gwt.event.shared.GwtEvent;
import org.lumongo.ui.shared.UIQueryObject;

/**
 * Created by Payam Meyer on 5/20/15.
 * @author pmeyer
 */
public class ExecuteSearchEvent extends GwtEvent<ExecuteSearchHandler> {
	public static final Type<ExecuteSearchHandler> TYPE = new Type<ExecuteSearchHandler>();

	private UIQueryObject uiQueryObject;

	public ExecuteSearchEvent(UIQueryObject uiQueryObject) {
		this.uiQueryObject = uiQueryObject;
	}

	@Override
	public Type<ExecuteSearchHandler> getAssociatedType() {
		return TYPE;
	}

	@Override
	protected void dispatch(ExecuteSearchHandler handler) {
		handler.handleExecuteSearch(uiQueryObject);
	}

}
