package org.lumongo.somongo.client.bus;

import com.google.gwt.event.shared.EventHandler;
import com.google.gwt.event.shared.GwtEvent;
import com.google.gwt.event.shared.GwtEvent.Type;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.event.shared.SimpleEventBus;

public class Bus {
	private static SimpleEventBus simpleEventBus;
	
	static {
		simpleEventBus = new SimpleEventBus();
	}
	
	public static void fireEvent(GwtEvent<?> event) {
		simpleEventBus.fireEvent(event);
	}
	
	public static <H extends EventHandler> HandlerRegistration addHandler(Type<H> type, final H handler) {
		return simpleEventBus.addHandler(type, handler);
	}
	
}
