package org.lumongo.ui.client.widgets.query;

import com.google.gwt.user.client.Window;
import gwt.material.design.addins.client.splitpanel.MaterialSplitPanel;
import gwt.material.design.client.constants.Color;
import gwt.material.design.client.ui.MaterialLabel;
import gwt.material.design.client.ui.MaterialPanel;
import gwt.material.design.client.ui.html.Div;
import gwt.material.design.client.ui.html.Paragraph;
import org.lumongo.ui.client.highlighter.Highlight;
import org.lumongo.ui.shared.UIQueryResults;

/**
 * Created by Payam Meyer on 3/21/17.
 * @author pmeyer
 */
public class QueryView extends Div {

	private final MaterialPanel leftPanel;
	private final MaterialPanel rightPanel;

	public QueryView() {
		MaterialSplitPanel splitPanel = new MaterialSplitPanel();
		splitPanel.setHeight(Window.getClientHeight() - 102 + "px");
		splitPanel.setBarPosition(25);
		leftPanel = new MaterialPanel();
		leftPanel.setBackgroundColor(Color.GREY_LIGHTEN_2);
		leftPanel.setGrid("s6 l3");
		leftPanel.add(new MaterialLabel("Left Stuff"));

		rightPanel = new MaterialPanel();
		rightPanel.setBackgroundColor(Color.GREY_LIGHTEN_2);
		rightPanel.setGrid("s6 l9");
		rightPanel.add(new MaterialLabel("Right Stuff"));

		splitPanel.add(leftPanel);
		splitPanel.add(rightPanel);

		add(splitPanel);
	}

	public void draw(UIQueryResults uiQueryResults) {
		leftPanel.clear();
		rightPanel.clear();

		leftPanel.add(new QueryOptionsView(uiQueryResults));

		if (!uiQueryResults.getJsonDocs().isEmpty()) {
			for (String jsonDoc : uiQueryResults.getJsonDocs()) {
				Div div = new Div();
				div.getElement().setInnerHTML(jsonDoc);
				Highlight.highlightBlock(div.getElement());
				rightPanel.add(div);
			}
		}
		else {
			rightPanel.add(new Paragraph("No results."));
		}

	}

}
