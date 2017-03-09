package org.lumongo.ui.client.eventbus;

import com.google.gwt.event.shared.GwtEvent;
import org.lumongo.ui.shared.UIQueryResults;

/**
 * Created by Payam Meyer on 5/20/15.
 * @author pmeyer
 */
public class SearchLoadedEvent extends GwtEvent<SearchLoadedHandler> {
	public static final Type<SearchLoadedHandler> TYPE = new Type<SearchLoadedHandler>();

	private UIQueryResults uiQueryResults;

	public SearchLoadedEvent(UIQueryResults uiQueryResults) {
		this.uiQueryResults = uiQueryResults;
	}

	@Override
	public Type<SearchLoadedHandler> getAssociatedType() {
		return TYPE;
	}

	@Override
	protected void dispatch(SearchLoadedHandler handler) {
		handler.handleSearchLoaded(uiQueryResults);
	}

}
