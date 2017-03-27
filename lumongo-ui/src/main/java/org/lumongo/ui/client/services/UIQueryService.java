package org.lumongo.ui.client.services;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;
import org.lumongo.ui.shared.InstanceInfo;
import org.lumongo.ui.shared.UIQueryObject;
import org.lumongo.ui.shared.UIQueryResults;

@RemoteServiceRelativePath("uiqueryservice")
public interface UIQueryService extends RemoteService {

	InstanceInfo getInstanceInfo() throws Exception;

	UIQueryResults search(String queryId) throws Exception;

	String saveQuery(UIQueryObject uiQueryObject) throws Exception;
}