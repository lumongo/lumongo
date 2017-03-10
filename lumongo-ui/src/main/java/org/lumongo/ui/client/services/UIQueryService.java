package org.lumongo.ui.client.services;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;
import org.lumongo.ui.shared.InstanceInfo;

@RemoteServiceRelativePath("uiqueryservice")
public interface UIQueryService extends RemoteService {

	InstanceInfo getInstanceInfo() throws Exception;
}