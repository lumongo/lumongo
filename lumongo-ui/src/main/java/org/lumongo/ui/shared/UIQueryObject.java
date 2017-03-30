package org.lumongo.ui.shared;

import com.google.gwt.user.client.rpc.IsSerializable;
import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Payam Meyer on 3/9/17.
 * @author pmeyer
 */
@Entity
public class UIQueryObject implements IsSerializable {

	@Id
	private ObjectId queryId;
	private String query;
	private Set<String> indexNames = Collections.emptySet();
	private boolean debug;
	private int start;
	private boolean dontCache;
	private Integer mm;
	private Boolean dismax;
	private Float dismaxTie;
	private List<String> queryFields = Collections.emptyList();
	private String defaultOperator;
	private Map<String, String> similarities = Collections.emptyMap();
	private List<String> filterQueries = Collections.emptyList();
	private List<String> cosineSimJsonList = Collections.emptyList();
	private List<String> filterJsonQueries = Collections.emptyList();
	private List<String> highlightList = Collections.emptyList();
	private List<String> highlightJsonList = Collections.emptyList();
	private List<String> analyzeJsonList = Collections.emptyList();
	private List<String> displayFields = Collections.emptyList();
	private List<String> facets = Collections.emptyList();
	private Boolean computeFacetError;
	private List<String> drillDowns;
	private Map<String, String> sortList = Collections.emptyMap();
	private int rows = 10;

	public UIQueryObject() {
	}

	public Set<String> getIndexNames() {
		if (indexNames.isEmpty()) {
			indexNames = new HashSet<>();
		}
		return indexNames;
	}

	public void setIndexNames(Set<String> indexNames) {
		this.indexNames = indexNames;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public boolean isDontCache() {
		return dontCache;
	}

	public void setDontCache(boolean dontCache) {
		this.dontCache = dontCache;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public Integer getMm() {
		return mm;
	}

	public void setMm(Integer mm) {
		this.mm = mm;
	}

	public Boolean isDismax() {
		return dismax;
	}

	public Boolean getDismax() {
		return dismax;
	}

	public void setDismax(Boolean dismax) {
		this.dismax = dismax;
	}

	public Float getDismaxTie() {
		return dismaxTie;
	}

	public void setDismaxTie(Float dismaxTie) {
		this.dismaxTie = dismaxTie;
	}

	public List<String> getQueryFields() {
		if (queryFields.isEmpty()) {
			queryFields = new ArrayList<>();
		}
		return queryFields;
	}

	public void setQueryFields(List<String> queryFields) {
		this.queryFields = queryFields;
	}

	public String getDefaultOperator() {
		return defaultOperator;
	}

	public void setDefaultOperator(String defaultOperator) {
		this.defaultOperator = defaultOperator;
	}

	public Map<String, String> getSimilarities() {
		return similarities;
	}

	public void setSimilarities(Map<String, String> similarities) {
		this.similarities = similarities;
	}

	public List<String> getFilterQueries() {
		return filterQueries;
	}

	public void setFilterQueries(List<String> filterQueries) {
		this.filterQueries = filterQueries;
	}

	public List<String> getCosineSimJsonList() {
		return cosineSimJsonList;
	}

	public void setCosineSimJsonList(List<String> cosineSimJsonList) {
		this.cosineSimJsonList = cosineSimJsonList;
	}

	public List<String> getFilterJsonQueries() {
		return filterJsonQueries;
	}

	public void setFilterJsonQueries(List<String> filterJsonQueries) {
		this.filterJsonQueries = filterJsonQueries;
	}

	public List<String> getHighlightList() {
		return highlightList;
	}

	public void setHighlightList(List<String> highlightList) {
		this.highlightList = highlightList;
	}

	public List<String> getHighlightJsonList() {
		return highlightJsonList;
	}

	public void setHighlightJsonList(List<String> highlightJsonList) {
		this.highlightJsonList = highlightJsonList;
	}

	public List<String> getAnalyzeJsonList() {
		return analyzeJsonList;
	}

	public void setAnalyzeJsonList(List<String> analyzeJsonList) {
		this.analyzeJsonList = analyzeJsonList;
	}

	public List<String> getDisplayFields() {
		if (displayFields.isEmpty()) {
			displayFields = new ArrayList<>();
		}
		return displayFields;
	}

	public void setDisplayFields(List<String> displayFields) {
		this.displayFields = displayFields;
	}

	public List<String> getFacets() {
		return facets;
	}

	public void setFacets(List<String> facets) {
		this.facets = facets;
	}

	public Boolean isComputeFacetError() {
		return computeFacetError;
	}

	public Boolean getComputeFacetError() {
		return computeFacetError;
	}

	public void setComputeFacetError(Boolean computeFacetError) {
		this.computeFacetError = computeFacetError;
	}

	public List<String> getDrillDowns() {
		return drillDowns;
	}

	public void setDrillDowns(List<String> drillDowns) {
		this.drillDowns = drillDowns;
	}

	public Map<String, String> getSortList() {
		return sortList;
	}

	public void setSortList(Map<String, String> sortList) {
		this.sortList = sortList;
	}

	public int getRows() {
		return rows;
	}

	public void setRows(int rows) {
		this.rows = rows;
	}
}
