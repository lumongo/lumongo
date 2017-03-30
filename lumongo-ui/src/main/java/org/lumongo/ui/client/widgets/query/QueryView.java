package org.lumongo.ui.client.widgets.query;

import com.google.gwt.event.logical.shared.ResizeEvent;
import com.google.gwt.event.logical.shared.ResizeHandler;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.ScrollPanel;
import gwt.material.design.addins.client.splitpanel.MaterialSplitPanel;
import gwt.material.design.client.constants.Color;
import gwt.material.design.client.ui.MaterialPanel;
import gwt.material.design.client.ui.html.Code;
import gwt.material.design.client.ui.html.Div;
import gwt.material.design.client.ui.html.Paragraph;
import gwt.material.design.client.ui.html.Pre;
import org.lumongo.ui.client.bundle.MainResources;
import org.lumongo.ui.client.highlighter.Highlight;
import org.lumongo.ui.shared.UIQueryResults;

/**
 * Created by Payam Meyer on 3/21/17.
 * @author pmeyer
 */
public class QueryView extends Div implements ResizeHandler {

	private final MaterialPanel leftPanel;
	private final MaterialPanel rightPanel;
	private ScrollPanel leftScrollPanel;
	private ScrollPanel rightScrollPanel;
	private final MaterialSplitPanel splitPanel;

	public QueryView() {
		splitPanel = new MaterialSplitPanel();
		splitPanel.setHeight(Window.getClientHeight() - 102 + "px");
		splitPanel.setBarPosition(25);
		leftPanel = new MaterialPanel();
		leftPanel.setBackgroundColor(Color.WHITE);
		leftPanel.setGrid("s6 l3");
		leftScrollPanel = new ScrollPanel();
		leftScrollPanel.setHeight(Window.getClientHeight() - 130 + "px");

		rightPanel = new MaterialPanel();
		rightPanel.setBackgroundColor(Color.GREY_LIGHTEN_2);
		rightPanel.setGrid("s6 l9");
		rightScrollPanel = new ScrollPanel();
		rightScrollPanel.setHeight(Window.getClientHeight() - 130 + "px");

		splitPanel.add(leftPanel);
		splitPanel.add(rightPanel);

		add(splitPanel);
	}

	public void draw(UIQueryResults uiQueryResults) {
		leftPanel.clear();
		rightPanel.clear();

		leftScrollPanel.setWidget(new QueryOptionsView(uiQueryResults));

		leftPanel.add(leftScrollPanel);

		Pre recordsDiv = new Pre();
		recordsDiv.addStyleName(MainResources.GSS.selectable());
		rightScrollPanel.setWidget(recordsDiv);
		rightPanel.add(rightScrollPanel);

		if (!uiQueryResults.getJsonDocs().isEmpty()) {
			for (String jsonDoc : uiQueryResults.getJsonDocs()) {
				Code code = new Code(jsonDoc);
				code.addStyleName(MainResources.GSS.borderBottom());
				Highlight.highlightBlock(code.getElement());
				recordsDiv.add(code);
			}
		}
		else {
			Paragraph noResultsPara = new Paragraph("No results.");
			noResultsPara.setMargin(25);
			recordsDiv.add(noResultsPara);
		}

	}

	@Override
	public void onResize(ResizeEvent event) {
		splitPanel.setHeight(Math.max(600, Window.getClientHeight() - 102) + "px");
		leftScrollPanel.setHeight(Math.max(600, Window.getClientHeight() - 130) + "px");
		rightScrollPanel.setHeight(Math.max(600, Window.getClientHeight() - 130) + "px");
	}
}
