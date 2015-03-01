package org.lumongo.example.twitter;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.apache.log4j.Logger;
import org.lumongo.util.LogUtil;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;

import java.io.IOException;

public class TwitterCrawler {
	@SuppressWarnings("unused")
	private final static Logger LOG = Logger.getLogger(TwitterCrawler.class);
	
	//TODO make these configurable
	private final static String CONSUMER_KEY = "xxx";
	private final static String CONSUMER_KEY_SECRET = "xxx";
	
	private final static String ACCESS_TOKEN = "xxx";
	private final static String ACCESS_TOKEN_SECRET = "xxx";
	
	public void start() throws TwitterException, IOException {
		Twitter twitter = new TwitterFactory().getInstance();
		twitter.setOAuthConsumer(CONSUMER_KEY, CONSUMER_KEY_SECRET);
		AccessToken accessToken = new AccessToken(ACCESS_TOKEN, ACCESS_TOKEN_SECRET);
		twitter.setOAuthAccessToken(accessToken);
		
		MongoClient mongo = new MongoClient();
		DB db = mongo.getDB("twitter");
		final DBCollection collection = db.getCollection("sample");
		
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				DBObject tweet = new BasicDBObject();
				
				tweet.put("_id", status.getId());
				tweet.put("createdAt", status.getCreatedAt());
				tweet.put("favoriteCount", status.getFavoriteCount());
				tweet.put("retweetCount", status.getRetweetCount());
				if (status.getGeoLocation() != null) {
					tweet.put("lat", status.getGeoLocation().getLatitude());
					tweet.put("long", status.getGeoLocation().getLongitude());
				}
				tweet.put("screenName", status.getUser().getScreenName());
				if (status.getRetweetedStatus() != null) {
					tweet.put("retweetedStatus", status.getRetweetedStatus().getText());
				}
				
				tweet.put("text", status.getText());
				
				DBObject query = new BasicDBObject();
				
				query.put("_id", status.getId());
				
				collection.update(query, tweet, true, false);
			}
			
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			}
			
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			}
			
			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
			
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				
			}
			
			@Override
			public void onStallWarning(StallWarning warning) {
				
			}
		};
		
		final TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.setOAuthConsumer(CONSUMER_KEY, CONSUMER_KEY_SECRET);
		twitterStream.setOAuthAccessToken(accessToken);
		twitterStream.addListener(listener);
		twitterStream.sample();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				twitterStream.cleanUp();
			}
		});
		
	}
	
	public static void main(String[] args) throws Exception {
		LogUtil.loadLogConfig();
		TwitterCrawler tm = new TwitterCrawler();
		tm.start();
		
	}
}
