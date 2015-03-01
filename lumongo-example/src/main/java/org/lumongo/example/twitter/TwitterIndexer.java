package org.lumongo.example.twitter;

import org.lumongo.client.command.Store;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.fields.Mapper;
import org.lumongo.util.LogUtil;

import java.util.Date;

public class TwitterIndexer {
	
	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();
		
		LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
		lumongoPoolConfig.addMember("localhost");
		
		LumongoWorkPool lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);
		try {
			Mapper<Tweet> mapper = new Mapper<Tweet>(Tweet.class);
			lumongoWorkPool.createOrUpdateIndex(mapper.createOrUpdateIndex());
			
			//sample tweet, lovely isnt it?
			Tweet tweet = new Tweet();
			tweet.setId("426145287804293120");
			tweet.setCreatedDate(new Date());
			tweet.setFavoriteCount(1);
			tweet.setRetweetCount(2);
			tweet.setUsername("maddawgggg34");
			tweet.setText("just wasted like $100 on 5sos merch ugh");
			tweet.setLatitude(40.68757493);
			tweet.setLatitude(-76.21184093);
			
			Store store = mapper.createStore(tweet);
			lumongoWorkPool.store(store);
		}
		finally {
			lumongoWorkPool.shutdown();
		}
	}
}
