package org.apache.lucene.util;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;

import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RateLimitedDirectoryWrapper;
import org.lumongo.storage.lucene.DistributedDirectory;
import org.lumongo.storage.lucene.MongoDirectory;
import org.lumongo.util.TestHelper;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class LumongoTestCase extends LuceneTestCase {
	
	private static int indexNameCounter = 0;
	
	@Override
	public void setUp() throws Exception {
		MongoDirectory.setMaxIndexBlocks(500);
		MongoClient mongo = TestHelper.getMongo();
		mongo.dropDatabase(TestHelper.TEST_DATABASE_NAME);
		mongo.close();
		super.setUp();
	}
	
	public static BaseDirectoryWrapper newDirectory() {
		return newDirectory(random());
	}
	
	public static MockDirectoryWrapper newMockDirectory() {
		return newMockDirectory(random());
	}
	
	public static BaseDirectoryWrapper newDirectory(Random random) {
		
		if (rarely(random)) {
			return newMockDirectory(random);
		}
		else {
			Directory directory = newDistributedDirectory(random);
			BaseDirectoryWrapper base = new BaseDirectoryWrapper(directory);
			closeAfterSuite(new CloseableDirectory(base, suiteFailureMarker));
			return base;
		}
	}
	
	public static MockDirectoryWrapper newMockDirectory(Random random) {
		Directory directory = newDistributedDirectory(random);
		MockDirectoryWrapper mock = new MockDirectoryWrapper(random, directory);
		mock.setThrottling(TEST_THROTTLING);
		closeAfterSuite(new CloseableDirectory(mock, suiteFailureMarker));
		return mock;
	}
	
	public static Directory newDistributedDirectory(Random random) {
		
		try {
			final MongoClient mongo = TestHelper.getMongo();
			Directory directory = new DistributedDirectory(
							new MongoDirectory(mongo, TestHelper.TEST_DATABASE_NAME, "index" + indexNameCounter++, false, false) {
								@Override
								public void close() {
									mongo.close();
								}
							});
			if (rarely(random)) {
				final double maxMBPerSec = 10 + 5 * (random.nextDouble() - 0.5);
				if (LuceneTestCase.VERBOSE) {
					System.out.println("LuceneTestCase: will rate limit output IndexOutput to " + maxMBPerSec + " MB/sec");
				}
				@SuppressWarnings("resource")
				final RateLimitedDirectoryWrapper rateLimitedDirectoryWrapper = new RateLimitedDirectoryWrapper(directory);
				switch (random.nextInt(10)) {
					case 3: // sometimes rate limit on flush
						rateLimitedDirectoryWrapper.setMaxWriteMBPerSec(maxMBPerSec, Context.FLUSH);
						break;
					case 2: // sometimes rate limit flush & merge
						rateLimitedDirectoryWrapper.setMaxWriteMBPerSec(maxMBPerSec, Context.FLUSH);
						rateLimitedDirectoryWrapper.setMaxWriteMBPerSec(maxMBPerSec, Context.MERGE);
						break;
					default:
						rateLimitedDirectoryWrapper.setMaxWriteMBPerSec(maxMBPerSec, Context.MERGE);
				}
				directory = rateLimitedDirectoryWrapper;
			}
			return directory;
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
	
	/**
	 * Returns a new Directory instance, with contents copied from the
	 * provided directory. See {@link #newDirectory()} for more
	 * information.
	 */
	public static BaseDirectoryWrapper newDirectory(Directory d) throws IOException {
		return newDirectory(random(), d);
	}
	
	/**
	 * Returns a new Directory instance, using the specified random
	 * with contents copied from the provided directory. See 
	 * {@link #newDirectory()} for more information.
	 */
	public static BaseDirectoryWrapper newDirectory(Random r, Directory d) throws IOException {
		BaseDirectoryWrapper directory = newDirectory();
		for (String file : d.listAll()) {
			d.copy(directory, file, file, newIOContext(r));
		}
		return directory;
	}
	
}
