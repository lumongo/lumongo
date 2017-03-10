package org.lumongo.ui.server;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import org.lumongo.ui.client.services.UIQueryService;
import org.lumongo.ui.shared.InstanceInfo;

/**
 * Created by Payam Meyer on 3/9/17.
 * @author pmeyer
 */
public class UIQueryServiceImpl extends RemoteServiceServlet implements UIQueryService {

	@Override
	public InstanceInfo getInstanceInfo() throws Exception {
		return null;
	}
}
