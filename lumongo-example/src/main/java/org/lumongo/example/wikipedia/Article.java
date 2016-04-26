package org.lumongo.example.wikipedia;


import org.lumongo.DefaultAnalyzers;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.Settings;
import org.lumongo.fields.annotations.UniqueId;

import java.util.Date;

@Settings(
		indexName = "wikipedia",
		numberOfSegments = 16,
		segmentCommitInterval = 6000)
public class Article {

	@UniqueId
	private String id;
	@Indexed(
			analyzerName = DefaultAnalyzers.STANDARD)
	private String title;
	@Indexed
	private Integer namespace;
	@DefaultSearch
	@Indexed(
			analyzerName = DefaultAnalyzers.STANDARD)
	private String text;

	@Indexed
	private Long revision;

	@Indexed
	private Integer userId;

	@Faceted
	@Indexed(
			analyzerName = DefaultAnalyzers.STANDARD)
	private String user;

	@Indexed
	private Date revisionDate;

	public Article() {

	}

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
