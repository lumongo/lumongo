package org.lumongo.example.twitter;

import org.apache.solr.client.solrj.beans.Field;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.fields.annotations.AsField;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.Settings;
import org.lumongo.fields.annotations.UniqueId;

import java.util.Date;

@Settings(
				indexName = Tweet.INDEX_NAME,
				numberOfSegments = 4,
				segmentCommitInterval = 6000)
public class Tweet {
	
	public static final String INDEX_NAME = "twitter";
	
	//using constant for field names is not required but allows referencing from
	//other parts of the code
	//if no field annotation is present, the name of the java field is used
	public static final String ID = "id";
	public static final String FAVORITE_COUNT = "favoriteCount";
	public static final String RETWEET_COUNT = "retweetCount";
	public static final String USER_NAME = "userName";
	public static final String CREATED_DATE = "createdDate";
	public static final String LATITUDE = "latitude";
	public static final String LONGITUDE = "longitude";
	
	public static final String TEXT = "text";
	
	public Tweet() {
		
	}
	
	@UniqueId
	@AsField(ID)
	private String id;
	
	@Indexed(
					analyzer = LMAnalyzer.NUMERIC_INT)
	@Field(FAVORITE_COUNT)
	private Integer favoriteCount;
	
	@Indexed(
					analyzer = LMAnalyzer.NUMERIC_INT)
	@Field(RETWEET_COUNT)
	private Integer retweetCount;
	
	@Indexed(
					analyzer = LMAnalyzer.LC_KEYWORD)
	@Field(USER_NAME)
	private String username;
	
	@Indexed(
					analyzer = LMAnalyzer.STANDARD)
	@Field(TEXT)
	@DefaultSearch
	private String text;
	
	@Indexed(
					analyzer = LMAnalyzer.NUMERIC_LONG)
	@Field(CREATED_DATE)
	private Date createdDate;
	
	@Indexed(
					analyzer = LMAnalyzer.NUMERIC_DOUBLE)
	@Field(LATITUDE)
	private Double latitude;
	
	@Indexed(
					analyzer = LMAnalyzer.NUMERIC_DOUBLE)
	@Field(LONGITUDE)
	private Double longitude;
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public Integer getFavoriteCount() {
		return favoriteCount;
	}
	
	public void setFavoriteCount(Integer favoriteCount) {
		this.favoriteCount = favoriteCount;
	}
	
	public Integer getRetweetCount() {
		return retweetCount;
	}
	
	public void setRetweetCount(Integer retweetCount) {
		this.retweetCount = retweetCount;
	}
	
	public String getUsername() {
		return username;
	}
	
	public void setUsername(String username) {
		this.username = username;
	}
	
	public String getText() {
		return text;
	}
	
	public void setText(String text) {
		this.text = text;
	}
	
	public Date getCreatedDate() {
		return createdDate;
	}
	
	public void setCreatedDate(Date createdDate) {
		this.createdDate = createdDate;
	}
	
	public Double getLatitude() {
		return latitude;
	}
	
	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}
	
	public Double getLongitude() {
		return longitude;
	}
	
	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}
	
}
