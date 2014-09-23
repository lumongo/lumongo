package org.lumongo.example.wikipedia;

import java.util.Date;

import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.Settings;
import org.lumongo.fields.annotations.UniqueId;

@Settings(
	indexName = "wikipedia",
	numberOfSegments = 16,
	segmentFlushInterval = 1000,
	segmentCommitInterval = 6000)
public class Article {
	
	public Article() {
		
	}
	
	@UniqueId
	private String id;
	
	@Indexed(
		analyzer = LMAnalyzer.STANDARD)
	private String title;
	
	@Indexed(
		analyzer = LMAnalyzer.NUMERIC_INT)
	private Integer namespace;
	
	@DefaultSearch
	@Indexed(
					analyzer = LMAnalyzer.STANDARD)
	private String text;
	
	@Indexed(
		analyzer = LMAnalyzer.NUMERIC_LONG)
	private Long revision;
	
	@Indexed(
					analyzer = LMAnalyzer.NUMERIC_INT)
	private Integer userId;
	
	@Faceted
	@Indexed(
					analyzer = LMAnalyzer.STANDARD)
	private String user;
	
	@Indexed(
		analyzer = LMAnalyzer.NUMERIC_LONG)
	private Date revisionDate;
	
	public String getTitle() {
		return title;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}
	
	public Integer getNamespace() {
		return namespace;
	}
	
	public void setNamespace(Integer namespace) {
		this.namespace = namespace;
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getText() {
		return text;
	}
	
	public void setText(String text) {
		this.text = text;
	}
	
	public Long getRevision() {
		return revision;
	}
	
	public void setRevision(Long revision) {
		this.revision = revision;
	}
	
	public Integer getUserId() {
		return userId;
	}
	
	public void setUserId(Integer userId) {
		this.userId = userId;
	}
	
	public String getUser() {
		return user;
	}
	
	public void setUser(String user) {
		this.user = user;
	}
	
	public Date getRevisionDate() {
		return revisionDate;
	}
	
	public void setRevisionDate(Date revisionDate) {
		this.revisionDate = revisionDate;
	}
	
}
