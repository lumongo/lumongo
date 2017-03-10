package org.lumongo.ui.client.services;

import com.google.gwt.user.client.rpc.AsyncCallback;
import org.lumongo.ui.shared.InstanceInfo;

public interface UIQueryServiceAsync {

	void getInstanceInfo(AsyncCallback<InstanceInfo> asyncCallback);
}