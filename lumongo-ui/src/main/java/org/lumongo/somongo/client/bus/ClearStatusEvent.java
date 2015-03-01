package org.lumongo.somongo.client.bus;

import com.google.gwt.event.shared.GwtEvent;

public class ClearStatusEvent extends GwtEvent<ClearStatusHandler> {
	public static final Type<ClearStatusHandler> TYPE = new Type<ClearStatusHandler>();
	
	@Override
	public com.google.gwt.event.shared.GwtEvent.Type<ClearStatusHandler> getAssociatedType() {
		return TYPE;
	}
	
	@Override
	protected void dispatch(ClearStatusHandler handler) {
		handler.clearStatus();
	}
	
}
