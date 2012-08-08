package org.lumongo.somongo.client;

import java.util.ArrayList;
import java.util.List;

import org.lumongo.somongo.client.css.DefaultClientBundle;
import org.lumongo.somongo.client.service.SearchService;
import org.lumongo.somongo.client.service.SearchServiceAsync;
import org.lumongo.somongo.shared.Document;
import org.lumongo.somongo.shared.SearchRequest;
import org.lumongo.somongo.shared.SearchResults;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Style.Unit;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.KeyCodes;
import com.google.gwt.event.dom.client.KeyDownEvent;
import com.google.gwt.event.dom.client.KeyDownHandler;
import com.google.gwt.event.logical.shared.SelectionEvent;
import com.google.gwt.event.logical.shared.SelectionHandler;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.SimplePager;
import com.google.gwt.user.cellview.client.TextColumn;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.DockLayoutPanel;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.RootLayoutPanel;
import com.google.gwt.user.client.ui.SuggestBox;
import com.google.gwt.user.client.ui.SuggestOracle.Suggestion;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.Range;
import com.google.gwt.view.client.RangeChangeEvent;

public class SoMongo implements EntryPoint {

    private final SearchServiceAsync searchService = GWT.create(SearchService.class);
    private ListBox indexList;
    private CellTable<Document> docCellTable;

    private SearchRequest lastRequest;

    @Override
    public void onModuleLoad() {
        DefaultClientBundle.INSTANCE.css().ensureInjected();

        DockLayoutPanel main = new DockLayoutPanel(Unit.PX);

        main.addNorth(createHeader(), 100);
        main.addWest(createNavigation(), 150);
        main.addSouth(createFooter(), 30);
        main.add(createMain());

        RootLayoutPanel.get().add(main);

    }


    protected Widget createHeader() {

        HorizontalPanel header = new HorizontalPanel();
        header.setWidth("100%");
        header.setHeight("100%");

        header.setVerticalAlignment(HasVerticalAlignment.ALIGN_MIDDLE);
        // header.add(new Image("images/logo.png"));

        HorizontalPanel searchPanel = new HorizontalPanel();


        final SuggestBox searchBox = new SuggestBox();
        searchBox.addKeyDownHandler(new KeyDownHandler() {
            @Override
            public void onKeyDown(KeyDownEvent event) {
                if (event.getNativeKeyCode() == KeyCodes.KEY_ENTER) {
                    searchDocuments(searchBox.getText());
                }
            }

        });

        searchBox.addSelectionHandler(new SelectionHandler<Suggestion>() {

            @Override
            public void onSelection(SelectionEvent<Suggestion> event) {
                searchDocuments(event.getSelectedItem().getReplacementString());
            }

        });

        Button searchButton = new Button("Search");
        searchButton.addClickHandler(new ClickHandler() {

            @Override
            public void onClick(ClickEvent event) {
                searchDocuments(searchBox.getText());
            }
        });
        searchPanel.setSpacing(10);
        searchPanel.add(searchBox);
        searchPanel.add(searchButton);

        header.setHorizontalAlignment(HasHorizontalAlignment.ALIGN_CENTER);
        header.add(searchPanel);
        return header;
    }

    private Widget createFooter() {
        HorizontalPanel footer = new HorizontalPanel();
        return footer;
    }

    protected Widget createNavigation() {
        VerticalPanel nav = new VerticalPanel();

        indexList = new ListBox(true);
        indexList.setVisibleItemCount(5);

        searchService.getIndexes(new AsyncCallback<List<String>>() {

            @Override
            public void onSuccess(List<String> result) {
                for (String r : result) {
                    indexList.addItem(r);
                }

            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Failed: " + caught);
            }
        });

        nav.add(new Label("Indexes"));
        nav.add(indexList);
        return nav;
    }

    protected Widget createMain() {
        VerticalPanel main = new VerticalPanel();

        docCellTable = new CellTable<Document>();

        TextColumn<Document> uniqueIdColumn = new TextColumn<Document>() {
            @Override
            public String getValue(Document document) {
                return document.getUniqueId();
            }
        };

        // Create address column.
        TextColumn<Document> scoreColumn = new TextColumn<Document>() {
            @Override
            public String getValue(Document document) {
                return (document.getScore() + "");
            }
        };

        SimplePager pager = new SimplePager();
        pager.setDisplay(docCellTable);

        docCellTable.addColumn(uniqueIdColumn);
        docCellTable.addColumn(scoreColumn);

        docCellTable.addRangeChangeHandler(new RangeChangeEvent.Handler() {

            @Override
            public void onRangeChange(RangeChangeEvent event) {
                Range range = event.getNewRange();
                final int start = range.getStart();
                final int length = range.getLength();
                lastRequest.setAmount(start + length);
                searchService.search(lastRequest, new AsyncCallback<SearchResults>() {

                    @Override
                    public void onFailure(Throwable caught) {

                    }

                    @Override
                    public void onSuccess(SearchResults result) {
                        docCellTable.setRowCount((int) result.getTotalHits());
                        docCellTable.setRowData(start, result.getDocuments().subList(start, start + length));
                    }

                });
            }

        });

        main.add(pager);
        main.add(docCellTable);
        return main;
    }

    protected void searchDocuments(String query) {
        query = query.trim();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setQuery(query);
        searchRequest.setAmount(20);
        searchRequest.setIndexes(getSelectedIndexes());
        lastRequest = searchRequest;
        searchService.search(searchRequest, new AsyncCallback<SearchResults>() {

            @Override
            public void onSuccess(SearchResults result) {
                docCellTable.setVisibleRange(0, 20);
                docCellTable.setRowCount((int) result.getTotalHits());
                docCellTable.setRowData(0, result.getDocuments());
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Failed: " + caught);
            }
        });

    }

    protected List<String> getSelectedIndexes() {
        List<String> selectedIndexes = new ArrayList<String>();
        for (int i = 0; i < indexList.getItemCount(); i++) {
            if (indexList.isItemSelected(i)) {
                selectedIndexes.add(indexList.getItemText(i));
            }
        }
        return selectedIndexes;

    }

}
