package org.lumongo.fields;

import org.lumongo.cluster.message.Lumongo;

import java.util.ArrayList;
import java.util.List;

public class FieldConfigBuilder {
	private String storedFieldName;
	private List<Lumongo.IndexAs> indexAsList;
	private List<Lumongo.FacetAs> facetAsList;
	private List<Lumongo.SortAs> sortAsList;

	public FieldConfigBuilder(String storedFieldName) {
		this.storedFieldName = storedFieldName;
		this.indexAsList = new ArrayList<>();
		this.facetAsList = new ArrayList<>();
		this.sortAsList = new ArrayList<>();
	}

	public static FieldConfigBuilder create(String storedFieldName) {
		return new FieldConfigBuilder(storedFieldName);
	}

	public FieldConfigBuilder indexAs(Lumongo.LMAnalyzer analyzer) {
		return indexAs(Lumongo.IndexAs.newBuilder().setIndexFieldName(storedFieldName).setAnalyzer(analyzer).build());
	}

	public FieldConfigBuilder indexAs(Lumongo.LMAnalyzer analyzer, String indexedFieldName) {
		return indexAs(Lumongo.IndexAs.newBuilder().setIndexFieldName(indexedFieldName).setAnalyzer(analyzer).build());
	}

	public FieldConfigBuilder indexAs(Lumongo.IndexAs indexAs) {
		this.indexAsList.add(indexAs);
		return this;
	}

	public FieldConfigBuilder facetAs(Lumongo.FacetAs.LMFacetType facetType) {
		return facetAs(facetType, storedFieldName);
	}

	public FieldConfigBuilder facetAs(Lumongo.FacetAs.LMFacetType facetType, String facetName) {
		return facetAs(Lumongo.FacetAs.newBuilder().setFacetName(facetName).setFacetType(facetType).build());
	}

	public FieldConfigBuilder facetAs(Lumongo.FacetAs facetAs) {
		this.facetAsList.add(facetAs);
		return this;
	}

	public FieldConfigBuilder sortAs(Lumongo.SortAs.SortType sortType) {
		return sortAs(sortType, storedFieldName);
	}

	public FieldConfigBuilder sortAs(Lumongo.SortAs.SortType sortType, String sortFieldName) {
		return sortAs(Lumongo.SortAs.newBuilder().setSortFieldName(sortFieldName).setSortType(sortType).build());
	}

	public FieldConfigBuilder sortAs(Lumongo.SortAs sortAs) {
		this.sortAsList.add(sortAs);
		return this;
	}

	public Lumongo.FieldConfig build() {
		Lumongo.FieldConfig.Builder fcBuilder = Lumongo.FieldConfig.newBuilder();
		fcBuilder.setStoredFieldName(storedFieldName);
		fcBuilder.addAllIndexAs(indexAsList);
		fcBuilder.addAllFacetAs(facetAsList);
		fcBuilder.addAllSortAs(sortAsList);

		return fcBuilder.build();
	}
}
