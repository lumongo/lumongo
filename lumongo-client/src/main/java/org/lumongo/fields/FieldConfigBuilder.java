package org.lumongo.fields;

import org.lumongo.cluster.message.Lumongo.FacetAs;
import org.lumongo.cluster.message.Lumongo.FacetAs.LMFacetType;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.IndexAs;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;

import java.util.ArrayList;
import java.util.List;

public class FieldConfigBuilder {
	private String storedFieldName;
	private List<IndexAs> indexAsList;
	private List<FacetAs> facetAsList;
	
	public static FieldConfigBuilder create(String storedFieldName) {
		return new FieldConfigBuilder(storedFieldName);
	}
	
	public FieldConfigBuilder(String storedFieldName) {
		this.storedFieldName = storedFieldName;
		this.indexAsList = new ArrayList<>();
		this.facetAsList = new ArrayList<>();
	}
	
	public FieldConfigBuilder indexAs(LMAnalyzer analyzer) {
		return indexAs(IndexAs.newBuilder().setIndexFieldName(storedFieldName).setAnalyzer(analyzer).build());
	}
	
	public FieldConfigBuilder indexAs(LMAnalyzer analyzer, String indexedFieldName) {
		return indexAs(IndexAs.newBuilder().setIndexFieldName(indexedFieldName).setAnalyzer(analyzer).build());
	}
	
	public FieldConfigBuilder indexAs(IndexAs indexAs) {
		this.indexAsList.add(indexAs);
		return this;
	}
	
	public FieldConfigBuilder facetAs(LMFacetType facetType) {
		return facetAs(FacetAs.newBuilder().setFacetName(storedFieldName).setFacetType(facetType).build());
	}
	
	public FieldConfigBuilder facetAs(LMFacetType facetType, String facetName) {
		return facetAs(FacetAs.newBuilder().setFacetName(facetName).setFacetType(facetType).build());
	}
	
	public FieldConfigBuilder facetAs(FacetAs facetAs) {
		this.facetAsList.add(facetAs);
		return this;
	}
	
	public FieldConfig build() {
		FieldConfig.Builder fcBuilder = FieldConfig.newBuilder();
		fcBuilder.setStoredFieldName(storedFieldName);
		fcBuilder.addAllIndexAs(indexAsList);
		fcBuilder.addAllFacetAs(facetAsList);
		return fcBuilder.build();
	}
}
