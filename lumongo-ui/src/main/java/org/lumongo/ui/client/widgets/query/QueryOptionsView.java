package org.lumongo.ui.client.widgets.query;

import com.google.gwt.event.dom.client.KeyCodes;
import com.google.gwt.user.client.rpc.AsyncCallback;
import gwt.material.design.client.constants.Color;
import gwt.material.design.client.constants.IconType;
import gwt.material.design.client.ui.MaterialBadge;
import gwt.material.design.client.ui.MaterialButton;
import gwt.material.design.client.ui.MaterialCheckBox;
import gwt.material.design.client.ui.MaterialCollapsible;
import gwt.material.design.client.ui.MaterialCollapsibleBody;
import gwt.material.design.client.ui.MaterialCollapsibleHeader;
import gwt.material.design.client.ui.MaterialCollapsibleItem;
import gwt.material.design.client.ui.MaterialColumn;
import gwt.material.design.client.ui.MaterialLabel;
import gwt.material.design.client.ui.MaterialLink;
import gwt.material.design.client.ui.MaterialListBox;
import gwt.material.design.client.ui.MaterialRow;
import gwt.material.design.client.ui.MaterialTextBox;
import gwt.material.design.client.ui.html.Div;
import gwt.material.design.client.ui.html.Option;
import org.lumongo.ui.client.bundle.MainResources;
import org.lumongo.ui.client.controllers.MainController;
import org.lumongo.ui.client.places.QueryPlace;
import org.lumongo.ui.client.services.ServiceProvider;
import org.lumongo.ui.client.widgets.base.CustomTextBox;
import org.lumongo.ui.client.widgets.base.ToastHelper;
import org.lumongo.ui.shared.IndexInfo;
import org.lumongo.ui.shared.UIQueryObject;
import org.lumongo.ui.shared.UIQueryResults;

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

	public QueryOptionsView(UIQueryResults uiQueryResults) {
		setMargin(15);
		setPadding(10);

		uiQueryObject = uiQueryResults.getUiQueryObject();
		if (uiQueryObject == null) {
			uiQueryObject = new UIQueryObject();
		}

		MaterialButton resetButton = new MaterialButton("Reset", IconType.REFRESH);
		resetButton.addClickHandler(clickEvent -> {
			MainController.get().goTo(new QueryPlace(null));
		});
		add(resetButton);

		if (!uiQueryResults.getJsonDocs().isEmpty()) {
			MaterialBadge resultsBadge = new MaterialBadge("Total Results: " + uiQueryResults.getTotalResults());
			add(resultsBadge);
		}

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
		searchBox.setPlaceHolder("q:");
		searchBox.setValue(uiQueryObject.getQuery());
		searchBox.addKeyUpHandler(clickEvent -> {
			if (clickEvent.getNativeKeyCode() == KeyCodes.KEY_ENTER) {
				uiQueryObject.setQuery(searchBox.getValue());
				runSearch(uiQueryObject);
			}
		});

		searchBox.getButton().addClickHandler(clickEvent -> {
			uiQueryObject.setQuery(searchBox.getValue());
			runSearch(uiQueryObject);
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
				runSearch(uiQueryObject);
			}
		});

		add(rowsIntegerBox);

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

		MaterialRow row = new MaterialRow();
		MaterialColumn leftColumn = new MaterialColumn(6, 6, 6);
		MaterialColumn rightColumn = new MaterialColumn(6, 6, 6);

		MaterialTextBox filterTextBox = new MaterialTextBox("Start typing to filter...");
		row.add(filterTextBox);

		row.add(leftColumn);
		row.add(rightColumn);
		body.add(row);

		leftColumn.add(new MaterialLabel("QF:"));
		rightColumn.add(new MaterialLabel("FL:"));

		List<MaterialCheckBox> qfCheckBoxes = new ArrayList<>();
		List<MaterialCheckBox> flCheckBoxes = new ArrayList<>();

		for (String fieldName : indexInfo.getFieldNames()) {
			MaterialCheckBox qfCheckBox = new MaterialCheckBox(fieldName);
			qfCheckBox.addStyleName(MainResources.GSS.wordBreakAll());
			if (uiQueryObject.getQueryFields().contains(fieldName)) {
				qfCheckBox.setValue(true);
			}
			qfCheckBox.addValueChangeHandler(event -> {
				if (event.getValue()) {
					uiQueryObject.getQueryFields().add(qfCheckBox.getText());
				}
				else {
					uiQueryObject.getQueryFields().remove(qfCheckBox.getText());
				}

			});
			qfCheckBoxes.add(qfCheckBox);
			leftColumn.add(qfCheckBox);

			MaterialCheckBox flCheckBox = new MaterialCheckBox(fieldName);
			flCheckBox.addStyleName(MainResources.GSS.wordBreakAll());
			if (uiQueryObject.getDisplayFields().contains(fieldName)) {
				flCheckBox.setValue(true);
			}
			flCheckBox.addValueChangeHandler(event -> {
				if (event.getValue()) {
					uiQueryObject.getDisplayFields().add(flCheckBox.getText());
				}
				else {
					uiQueryObject.getDisplayFields().remove(flCheckBox.getText());
				}

			});
			flCheckBoxes.add(flCheckBox);
			rightColumn.add(flCheckBox);
		}

		filterTextBox.addKeyUpHandler(keyUpEvent -> {
			for (MaterialCheckBox qfCheckBox : qfCheckBoxes) {
				if (!qfCheckBox.getText().contains(filterTextBox.getValue())) {
					qfCheckBox.setVisible(false);
				}
				else {
					qfCheckBox.setVisible(true);
				}
			}

			for (MaterialCheckBox flCheckBox : flCheckBoxes) {
				if (!flCheckBox.getText().contains(filterTextBox.getValue())) {
					flCheckBox.setVisible(false);
				}
				else {
					flCheckBox.setVisible(true);
				}
			}
		});

		item.add(header);
		item.add(body);

		fieldNameCollapsible.add(item);
	}

	private void runSearch(UIQueryObject uiQueryObject) {
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
