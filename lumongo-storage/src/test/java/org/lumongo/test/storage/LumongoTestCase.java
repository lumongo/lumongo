package org.lumongo.test.storage;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.lumongo.storage.lucene.DistributedDirectory;
import org.lumongo.storage.lucene.MongoDirectory;
import org.lumongo.util.TestHelper;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class LumongoTestCase extends LuceneTestCase {
	
	private static int indexNameCounter = 0;
	
	@Override
	public void setUp() throws Exception {
		MongoClient mongo = TestHelper.getMongo();
		mongo.dropDatabase(TestHelper.TEST_DATABASE_NAME);
		mongo.close();
		super.setUp();
	}
	
	public static BaseDirectoryWrapper newDirectory() {
		
		try {
			final MongoClient mongo = TestHelper.getMongo();
			return new BaseDirectoryWrapper(new DistributedDirectory(new MongoDirectory(mongo, TestHelper.TEST_DATABASE_NAME, "index" + indexNameCounter++,
							false, false) {
				@Override
				public void close() {
					mongo.close();
				}
			}));
		}
		catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
		catch (MongoException e) {
			throw new RuntimeException(e);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}
	
}
