package org.lumongo.ui.client.widgets.query;

import com.google.gwt.event.dom.client.KeyCodes;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.TextBox;
import gwt.material.design.client.constants.Color;
import gwt.material.design.client.constants.IconType;
import gwt.material.design.client.ui.MaterialLink;
import gwt.material.design.client.ui.MaterialListBox;
import gwt.material.design.client.ui.html.Div;
import gwt.material.design.client.ui.html.Option;
import org.lumongo.ui.client.bundle.MainResources;
import org.lumongo.ui.client.controllers.MainController;
import org.lumongo.ui.client.places.QueryPlace;
import org.lumongo.ui.client.services.ServiceProvider;
import org.lumongo.ui.client.widgets.base.ToastHelper;
import org.lumongo.ui.shared.IndexInfo;
import org.lumongo.ui.shared.UIQueryObject;
import org.lumongo.ui.shared.UIQueryResults;

/**
 * Created by Payam Meyer on 3/27/17.
 * @author pmeyer
 */
public class QueryOptionsView extends Div {

	private UIQueryObject uiQueryObject;

	public QueryOptionsView(UIQueryResults uiQueryResults) {
		setMargin(15);
		setPadding(10);

		uiQueryObject = uiQueryResults.getUiQueryObject();
		if (uiQueryObject == null) {
			uiQueryObject = new UIQueryObject();
		}

		MaterialListBox indexesListBox = new MaterialListBox();
		indexesListBox.setMultipleSelect(true);
		Option selectOneIndexOption = new Option("Select Indexes");
		selectOneIndexOption.setDisabled(true);
		indexesListBox.add(selectOneIndexOption);

		MaterialListBox queryFieldsListBox = new MaterialListBox();
		queryFieldsListBox.setMultipleSelect(true);
		Option selectOneQFOption = new Option("Select Query Fields");
		selectOneQFOption.setDisabled(true);
		queryFieldsListBox.add(selectOneQFOption);

		for (IndexInfo indexInfo : uiQueryResults.getIndexes()) {
			Option option = new Option(indexInfo.getName());
			if (uiQueryObject.getIndexNames().contains(indexInfo.getName())) {
				option.setSelected(true);
			}
			indexesListBox.add(option);

			for (String fieldName : indexInfo.getFieldNames()) {
				Option fieldNameOption = new Option();
				fieldNameOption.setLabel(indexInfo.getName());
				fieldNameOption.setText(fieldName);
				queryFieldsListBox.add(fieldNameOption);
			}
		}
		indexesListBox.addValueChangeHandler(valueChangeEvent -> uiQueryObject.getIndexNames().add(valueChangeEvent.getValue()));
		queryFieldsListBox.addValueChangeHandler(valueChangeEvent -> {
			uiQueryObject.getQueryFields().add(valueChangeEvent.getValue());
		});
		add(indexesListBox);
		add(queryFieldsListBox);
		//indexesListBox.setHelperText("Select an Index");

		Div searchDiv = new Div();
		searchDiv.setMargin(3);
		searchDiv.setBackgroundColor(Color.WHITE);
		searchDiv.addStyleName(MainResources.GSS.searchBox());

		TextBox searchBox = new TextBox();
		searchBox.setStyleName(MainResources.GSS.searchBoxInput());
		searchBox.getElement().setAttribute("placeholder", "q:");
		searchBox.addKeyUpHandler(clickEvent -> {
			if (clickEvent.getNativeKeyCode() == KeyCodes.KEY_ENTER) {
				uiQueryObject.setQuery(searchBox.getText());
				runSearch(uiQueryObject);
			}
		});
		searchDiv.add(searchBox);

		MaterialLink searchButton = new MaterialLink();
		searchButton.setIconType(IconType.SEARCH);
		searchButton.setIconColor(Color.GREY);
		searchButton.addClickHandler(clickEvent -> {
			uiQueryObject.setQuery(searchBox.getText());
			runSearch(uiQueryObject);
		});

		searchDiv.add(searchButton);

		add(searchDiv);

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
