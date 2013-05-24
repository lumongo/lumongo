package org.lumongo.example.medline;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.fields.annotations.DefaultSearch;
import org.lumongo.fields.annotations.Faceted;
import org.lumongo.fields.annotations.Indexed;
import org.lumongo.fields.annotations.Saved;
import org.lumongo.fields.annotations.Settings;
import org.lumongo.fields.annotations.UniqueId;

@Settings(indexName = "medline", numberOfSegments = 16, segmentFlushInterval = 1500, segmentCommitInterval = 6000)
public class Document {

    @DefaultSearch
    @Saved
    @Indexed(LMAnalyzer.STANDARD)
    private String title;

    @Saved
    @Indexed(LMAnalyzer.STANDARD)
    private String journalTitle;

    @Saved
    @Indexed(LMAnalyzer.STANDARD)
    private String abstractText;

    @Saved
    @Indexed(LMAnalyzer.NUMERIC_LONG)
    private Date publicationDate;

    @Saved
    @Indexed(LMAnalyzer.LC_KEYWORD)
    private String journalVolume;

    @Saved
    @Indexed(LMAnalyzer.LC_KEYWORD)
    private String journalIssue;

    @Saved
    @Faceted
    @Indexed(LMAnalyzer.LC_KEYWORD)
    private String journalCountry;

    @Saved
    @Faceted
    @Indexed(LMAnalyzer.LC_KEYWORD)
    private String issn;

    @Saved
    @Indexed(LMAnalyzer.STANDARD)
    private List<String> authors;

    @Saved
    @Faceted
    @Indexed(LMAnalyzer.LC_KEYWORD)
    private List<String> authorsExact;

    @UniqueId
    private String pmid;

    public String getPmid() {
        return pmid;
    }

    public void setPmid(String pmid) {
        this.pmid = pmid;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAbstractText() {
        return abstractText;
    }

    public void setAbstractText(String abstractText) {
        this.abstractText = abstractText;
    }

    public String getIssn() {
        return issn;
    }

    public void setIssn(String issn) {
        this.issn = issn;
    }

    public String getJournalTitle() {
        return journalTitle;
    }

    public void setJournalTitle(String journalTitle) {
        this.journalTitle = journalTitle;
    }

    public Date getPublicationDate() {
        return publicationDate;
    }

    public void setPublicationDate(Date publicationDate) {
        this.publicationDate = publicationDate;
    }

    public String getJournalVolume() {
        return journalVolume;
    }

    public void setJournalVolume(String journalVolume) {
        this.journalVolume = journalVolume;
    }

    public String getJournalIssue() {
        return journalIssue;
    }

    public void setJournalIssue(String journalIssue) {
        this.journalIssue = journalIssue;
    }

    public String getJournalCountry() {
        return journalCountry;
    }

    public void setJournalCountry(String journalCountry) {
        this.journalCountry = journalCountry;
    }

    public void addAuthor(String author) {
        if (this.authors == null) {
            this.authors = new ArrayList<String>();
        }
        if (this.authorsExact == null) {
            this.authorsExact = new ArrayList<String>();
        }

        this.authors.add(author);
        this.authorsExact.add(author);

    }

    @Override
    public String toString() {
        return "Document [title=" + title + ", journalTitle=" + journalTitle + ", abstractText=" + abstractText + ", publicationDate=" + publicationDate
                + ", journalVolume=" + journalVolume + ", journalIssue=" + journalIssue + ", journalCountry=" + journalCountry + ", issn=" + issn
                + ", authors=" + authors + ", authorsExact=" + authorsExact + ", pmid=" + pmid + "]\n";
    }

}
