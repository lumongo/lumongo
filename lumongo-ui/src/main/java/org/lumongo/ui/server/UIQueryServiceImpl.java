package org.lumongo.ui.server;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import org.lumongo.client.command.GetFields;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.ui.client.services.UIQueryService;
import org.lumongo.ui.shared.IndexInfo;
import org.lumongo.ui.shared.InstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Payam Meyer on 3/9/17.
 * @author pmeyer
 */
public class UIQueryServiceImpl extends RemoteServiceServlet implements UIQueryService {

	private static final Logger LOG = LoggerFactory.getLogger(UIQueryServiceImpl.class);
	private static final int MB = 1024 * 1024;
	private LumongoWorkPool lumongoWorkPool;
	private String lumongoVersion;
	private String luceneVersion;

	@Override
	public void init() throws ServletException {
		super.init();

		try {
			LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig().addMember("localhost").setDefaultRetries(2);
			lumongoPoolConfig.setMemberUpdateEnabled(false);
			lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);

			Properties properties = new Properties();
			properties.load(UIQueryServiceImpl.class.getResourceAsStream("/version.properties"));
			lumongoVersion = properties.getProperty("lumongoVersion");
			luceneVersion = properties.getProperty("luceneVersion");
		}
		catch (Exception e) {
			LOG.error("Failed to initiate lumongo work pool.", e);
		}

	}

	@Override
	public InstanceInfo getInstanceInfo() throws Exception {
		InstanceInfo instanceInfo = new InstanceInfo();
		instanceInfo.setLumongoVersion(lumongoVersion);
		instanceInfo.setLuceneVersion(luceneVersion);

		GetIndexesResult indexes = lumongoWorkPool.getIndexes();

		List<IndexInfo> indexInfoList = new ArrayList<>();
		for (String indexName : indexes.getIndexNames()) {
			IndexInfo indexInfo = new IndexInfo();
			indexInfo.setName(indexName);
			indexInfo.setSize(20L);
			indexInfo.setTotalDocs((int) lumongoWorkPool.getNumberOfDocs(indexName).getNumberOfDocs());
			indexInfo.setFieldNames(new ArrayList<>(lumongoWorkPool.getFields(new GetFields(indexName)).getFieldNames()));

			indexInfoList.add(indexInfo);
		}

		instanceInfo.setIndexes(indexInfoList);

		Runtime runtime = Runtime.getRuntime();

		// TODO: These need to be LuMongo's not this app, who cares about this app?
		instanceInfo.setJvmUsedMemory((runtime.totalMemory() - runtime.freeMemory()) / MB);
		instanceInfo.setJvmFreeMemory(runtime.freeMemory() / MB);
		instanceInfo.setJvmTotalMemoryMB(runtime.totalMemory() / MB);
		instanceInfo.setJvmMaxMemoryMB(runtime.maxMemory() / MB);

		return instanceInfo;
	}
}