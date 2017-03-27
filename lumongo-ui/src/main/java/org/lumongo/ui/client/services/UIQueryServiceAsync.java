package org.lumongo.ui.client.services;

import com.google.gwt.user.client.rpc.AsyncCallback;
import org.lumongo.ui.shared.InstanceInfo;
import org.lumongo.ui.shared.UIQueryObject;
import org.lumongo.ui.shared.UIQueryResults;

public interface UIQueryServiceAsync {

	void getInstanceInfo(AsyncCallback<InstanceInfo> asyncCallback);

	void search(String queryId, AsyncCallback<UIQueryResults> asyncCallback);

	void saveQuery(UIQueryObject uiQueryObject, AsyncCallback<String> asyncCallback);
}