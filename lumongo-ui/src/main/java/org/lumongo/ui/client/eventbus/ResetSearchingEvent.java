package org.lumongo.ui.client.eventbus;

import com.google.gwt.event.shared.GwtEvent;

/**
 * Created by pmeyer on 5/20/15.
 */
public class ResetSearchingEvent extends GwtEvent<ResetSearchingHandler> {
	public static final Type<ResetSearchingHandler> TYPE = new Type<ResetSearchingHandler>();

	public ResetSearchingEvent() {
	}

	@Override
	public Type<ResetSearchingHandler> getAssociatedType() {
		return TYPE;
	}

	@Override
	protected void dispatch(ResetSearchingHandler handler) {
		handler.handleResetSearching();
	}

}
