package org.lumongo.test.cluster;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.lumongo.client.command.CreateOrUpdateIndex;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.CreateOrUpdateIndexResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.FacetAs.LMFacetType;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.doc.ResultDocBuilder;
import org.lumongo.fields.FieldConfigBuilder;
import org.lumongo.util.LogUtil;

import java.util.Date;

public class DateTest {
	public static void main(String[] args) {
		ServerTestBase serverTest = new ServerTestBase();

		try {
			LogUtil.loadLogConfig();
			serverTest.startSuite(1);
			Thread.sleep(1000);
		}
		catch (Exception e1) {
			e1.printStackTrace();
		}
		
		//create the connection pool
		LumongoWorkPool lumongoWorkPool = serverTest.getLumongoWorkPool();
		
		try {
			
			String defaultSearchField = "content";
			
			IndexConfig indexConfig = new IndexConfig(defaultSearchField);
			
			indexConfig.addFieldConfig(FieldConfigBuilder.create("title").indexAs(LMAnalyzer.STANDARD).facetAs(LMFacetType.STANDARD));
			indexConfig.addFieldConfig(FieldConfigBuilder.create("date").indexAs(LMAnalyzer.DATE).facetAs(LMFacetType.STANDARD));
			
			CreateOrUpdateIndex createOrUpdateIndex = new CreateOrUpdateIndex(SingleNodeTest.MY_TEST_INDEX, 2, indexConfig);
			@SuppressWarnings("unused")
			CreateOrUpdateIndexResult result = lumongoWorkPool.createOrUpdateIndex(createOrUpdateIndex);
			DBObject dbObject = new BasicDBObject();
			
			dbObject.put("title", "this is a fancy title");
			
			dbObject.put("date", new Date());
			
			Store s = new Store("id1", SingleNodeTest.MY_TEST_INDEX);
			s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(dbObject));
			lumongoWorkPool.store(s);
			
			Query query = new Query(SingleNodeTest.MY_TEST_INDEX, "*:*", 10);
			query.addFieldSort("date", Direction.ASCENDING);
			QueryResult queryResult = lumongoWorkPool.query(query);
			
			System.out.println(queryResult.getTotalHits());
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				serverTest.stopSuite();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}
}
