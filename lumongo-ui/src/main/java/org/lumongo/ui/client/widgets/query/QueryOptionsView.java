package org.lumongo.ui.client.widgets.query;

import com.google.gwt.event.dom.client.KeyCodes;
import com.google.gwt.i18n.client.NumberFormat;
import com.google.gwt.user.client.rpc.AsyncCallback;
import gwt.material.design.client.constants.Color;
import gwt.material.design.client.constants.IconType;
import gwt.material.design.client.constants.TabType;
import gwt.material.design.client.ui.MaterialBadge;
import gwt.material.design.client.ui.MaterialButton;
import gwt.material.design.client.ui.MaterialCollapsible;
import gwt.material.design.client.ui.MaterialCollapsibleBody;
import gwt.material.design.client.ui.MaterialCollapsibleHeader;
import gwt.material.design.client.ui.MaterialCollapsibleItem;
import gwt.material.design.client.ui.MaterialLink;
import gwt.material.design.client.ui.MaterialListBox;
import gwt.material.design.client.ui.MaterialTabItem;
import gwt.material.design.client.ui.html.Br;
import gwt.material.design.client.ui.html.Div;
import gwt.material.design.client.ui.html.Option;
import org.lumongo.ui.client.controllers.MainController;
import org.lumongo.ui.client.places.QueryPlace;
import org.lumongo.ui.client.services.ServiceProvider;
import org.lumongo.ui.client.widgets.base.CustomTabPanel;
import org.lumongo.ui.client.widgets.base.CustomTextBox;
import org.lumongo.ui.client.widgets.base.ToastHelper;
import org.lumongo.ui.shared.IndexInfo;
import org.lumongo.ui.shared.UIQueryObject;
import org.lumongo.ui.shared.UIQueryResults;
import org.lumongo.ui.shared.UIUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Payam Meyer on 3/27/17.
 * @author pmeyer
 */
public class QueryOptionsView extends Div {

	private UIQueryObject uiQueryObject;
	private final MaterialCollapsible fieldNameCollapsible;
	private final Map<String, MaterialCollapsibleItem> fieldItems = new HashMap<>();
	private final Div filterQueryDiv;
	private List<FilterQueryWidget> filterQueries = new ArrayList<>();

	public QueryOptionsView(UIQueryResults uiQueryResults) {
		setMargin(15);
		setPadding(10);

		uiQueryObject = uiQueryResults.getUiQueryObject();
		if (uiQueryObject == null) {
			uiQueryObject = new UIQueryObject();
		}

		if (!uiQueryResults.getJsonDocs().isEmpty()) {
			MaterialBadge resultsBadge = new MaterialBadge("Total Results: " + NumberFormat.getFormat("#,##0").format(uiQueryResults.getTotalResults()));
			add(resultsBadge);
			add(new Br());
		}

		MaterialButton executeButton = new MaterialButton("Execute", IconType.SEARCH);
		executeButton.addClickHandler(clickEvent -> runSearch());
		executeButton.setMarginRight(2);
		add(executeButton);

		MaterialButton resetButton = new MaterialButton("Reset", IconType.REFRESH);
		resetButton.addClickHandler(clickEvent -> MainController.get().goTo(new QueryPlace(null)));
		add(resetButton);

		MaterialListBox indexesListBox = new MaterialListBox();
		indexesListBox.setMultipleSelect(true);
		Option selectOneIndexOption = new Option("Select Indexes");
		selectOneIndexOption.setDisabled(true);
		indexesListBox.add(selectOneIndexOption);

		fieldNameCollapsible = new MaterialCollapsible();
		fieldNameCollapsible.setAccordion(false);
		for (IndexInfo indexInfo : uiQueryResults.getIndexes()) {
			createFieldNameCollapsible(indexInfo);

			Option option = new Option(indexInfo.getName());
			if (uiQueryObject.getIndexNames().contains(indexInfo.getName())) {
				option.setSelected(true);
				fieldItems.get(indexInfo.getName()).setVisible(true);
			}
			indexesListBox.add(option);
		}
		indexesListBox.addValueChangeHandler(valueChangeEvent -> {
			for (String indexName : fieldItems.keySet()) {
				fieldItems.get(indexName).setVisible(false);
			}
			for (String itemsSelected : indexesListBox.getItemsSelected()) {
				uiQueryObject.getIndexNames().add(itemsSelected);
				fieldItems.get(itemsSelected).setVisible(true);
			}
		});

		add(indexesListBox);
		add(fieldNameCollapsible);

		CustomTextBox searchBox = new CustomTextBox(true);
		searchBox.setPlaceHolder("q=*:*");
		searchBox.setValue(uiQueryObject.getQuery());
		searchBox.addKeyUpHandler(clickEvent -> {
			if (clickEvent.getNativeKeyCode() == KeyCodes.KEY_ENTER) {
				uiQueryObject.setQuery(searchBox.getValue());
				runSearch();
			}
		});

		searchBox.getButton().setTitle("Execute Query");
		searchBox.getButton().addClickHandler(clickEvent -> {
			uiQueryObject.setQuery(searchBox.getValue());
			runSearch();
		});

		add(searchBox);

		CustomTextBox rowsIntegerBox = new CustomTextBox();
		rowsIntegerBox.setPlaceHolder("rows (defaults to 10)");
		if (uiQueryObject != null && uiQueryObject.getRows() != 10) {
			rowsIntegerBox.setValue(uiQueryObject.getRows() + "");
		}
		rowsIntegerBox.getTextBox().addChangeHandler(changeEvent -> uiQueryObject.setRows(Integer.valueOf(rowsIntegerBox.getValue())));
		rowsIntegerBox.addKeyUpHandler(clickEvent -> {
			if (clickEvent.getNativeKeyCode() == KeyCodes.KEY_ENTER) {
				runSearch();
			}
		});

		add(rowsIntegerBox);

		filterQueryDiv = new Div();
		if (!uiQueryObject.getFilterQueries().isEmpty()) {
			for (String fq : uiQueryObject.getFilterQueries()) {
				createFilterQueryWidget(fq);
			}
		}
		else {
			createFilterQueryWidget(null);
		}

		add(filterQueryDiv);

	}

	private void createFilterQueryWidget(String fq) {

		FilterQueryWidget filterQueryWidget = new FilterQueryWidget();
		filterQueryWidget.setPlaceHolder("fq=*:*");
		filterQueryWidget.getPlusButton().addClickHandler(clickEvent -> createFilterQueryWidget(null));
		if (fq != null) {
			filterQueryWidget.getTextBox().setValue(fq);
		}

		filterQueryWidget.getMinusButton().addClickHandler(clickEvent -> {
			if (filterQueryDiv.getWidgetCount() > 1) {
				filterQueryDiv.remove(filterQueryWidget);
				filterQueries.remove(filterQueryWidget);
				uiQueryObject.getFilterQueries().remove(filterQueryWidget.getValue());
			}
			else {
				uiQueryObject.getFilterQueries().remove(filterQueryWidget.getValue());
				filterQueryWidget.getTextBox().setValue("");
			}
		});

		filterQueries.add(filterQueryWidget);

		filterQueryDiv.add(filterQueryWidget);

	}

	private void createFieldNameCollapsible(IndexInfo indexInfo) {
		MaterialCollapsibleItem item = new MaterialCollapsibleItem();
		item.setVisible(false);
		fieldItems.put(indexInfo.getName(), item);

		MaterialCollapsibleHeader header = new MaterialCollapsibleHeader();
		header.setBackgroundColor(Color.GREY_LIGHTEN_1);
		MaterialLink link = new MaterialLink("'" + indexInfo.getName() + "' fields");
		link.setTextColor(Color.WHITE);
		header.add(link);

		MaterialCollapsibleBody body = new MaterialCollapsibleBody();
		body.setPaddingTop(0);
		body.setBackgroundColor(Color.WHITE);

		CustomTabPanel tabPanel = new CustomTabPanel(TabType.DEFAULT);
		tabPanel.setMarginLeft(-40);
		tabPanel.setMarginRight(-40);

		String qfTabId = UIUtil.getRandomId();
		MaterialTabItem qfTabItem = tabPanel.createAndAddTabListItem("QF", "#" + qfTabId);
		tabPanel.createAndAddTabPane(qfTabId, new FieldsDiv(indexInfo.getQfList(), uiQueryObject.getQueryFields()));
		header.addClickHandler(clickEvent -> qfTabItem.selectTab());

		String flTabId = UIUtil.getRandomId();
		tabPanel.createAndAddTabListItem("FL", "#" + flTabId);
		tabPanel.createAndAddTabPane(flTabId, new FieldsDiv(indexInfo.getFlList(), uiQueryObject.getDisplayFields()));

		String facetsTabId = UIUtil.getRandomId();
		tabPanel.createAndAddTabListItem("Facets", "#" + facetsTabId);
		tabPanel.createAndAddTabPane(facetsTabId, new FieldsDiv(indexInfo.getFacetList(), uiQueryObject.getFacets()));

		body.add(tabPanel);

		item.add(header);
		item.add(body);

		fieldNameCollapsible.add(item);
	}

	private void runSearch() {
		for (FilterQueryWidget fqw : filterQueries) {
			if (!fqw.getValue().isEmpty()) {
				uiQueryObject.getFilterQueries().add(fqw.getValue());
			}
		}
		ServiceProvider.get().getLumongoService().saveQuery(uiQueryObject, new AsyncCallback<String>() {
			@Override
			public void onFailure(Throwable caught) {
				ToastHelper.showFailure("Failed to run the query.", caught);
			}

			@Override
			public void onSuccess(String result) {
				MainController.get().goTo(new QueryPlace(result));
			}
		});
	}

}
