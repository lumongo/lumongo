package org.lumongo.example.medline;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.joda.time.DateTime;
import org.lumongo.example.medline.schema.*;
import org.lumongo.xml.StaxJAXBReader;

import javax.xml.bind.JAXBException;
import java.text.DateFormatSymbols;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public abstract class MedlineJAXBReader extends StaxJAXBReader<MedlineCitation> {
	
	private static List<String> shortMonths = Arrays.asList(new DateFormatSymbols().getShortMonths());
	private static Joiner joiner = Joiner.on(",");

	protected MedlineJAXBReader(Class<MedlineCitation> documentJAXBClass, String documentElementName) throws JAXBException {
		super(documentJAXBClass, documentElementName);
	}
	
	@Override
	public void handle(MedlineCitation item) throws Exception {
		final MedlineDocument document = handleRecord(item);
		handleMedlineDocument(document);
	}
	
	public abstract void handleMedlineDocument(MedlineDocument document) throws Exception;
	
	public static MedlineDocument handleRecord(final MedlineCitation medlineCitation) throws Exception {
		MedlineDocument document = new MedlineDocument();
		
		Article article = medlineCitation.getArticle();
		AuthorList authorList = article.getAuthorList();
		if (authorList != null) {
			for (Author author : authorList.getAuthor()) {
				String valid = author.getValidYN();
				if (valid != null && valid.equals("Y")) {
					List<Object> names = author.getLastNameOrForeNameOrInitialsOrSuffixOrCollectiveName();
					
					String firstName = null;
					@SuppressWarnings("unused")
					String initials = null;
					String lastName = null;
					String suffix = null;
					String collectiveName = null;
					
					for (Object name : names) {
						if (name instanceof LastName) {
							LastName ln = (LastName) name;
							lastName = ln.getvalue();
						}
						else if (name instanceof ForeName) {
							ForeName fn = (ForeName) name;
							firstName = fn.getvalue();
						}
						else if (name instanceof Initials) {
							Initials it = (Initials) name;
							initials = it.getvalue();
						}
						else if (name instanceof Suffix) {
							Suffix sf = (Suffix) name;
							suffix = sf.getvalue();
						}
						else if (name instanceof CollectiveName) {
							CollectiveName cn = (CollectiveName) name;
							collectiveName = cn.getvalue();
						}
						
					}
					
					if (collectiveName != null) {
						document.addAuthor(collectiveName);
					}
					if (firstName != null && lastName != null && suffix != null) {
						String authorName = firstName + " " + lastName + " " + suffix;
						document.addAuthor(authorName);
					}
					if (firstName != null && lastName != null) {
						String authorName = firstName + " " + lastName;
						document.addAuthor(authorName);
					}
					
				}
			}
		}
		List<Object> paginationOrELocationID = medlineCitation.getArticle().getPaginationOrELocationID();
		StringBuilder fullPagination = new StringBuilder();
		for (Object obj : paginationOrELocationID) {
			if (obj instanceof Pagination) {
				Pagination page = (Pagination) obj;
				List<Object> startPageOrEndPageOrMedlinePgn = page.getStartPageOrEndPageOrMedlinePgn();
				String start = null;
				String end = null;
				for (Object o : startPageOrEndPageOrMedlinePgn) {
					if (o instanceof StartPage) {
						StartPage startPage = (StartPage) o;
						start = startPage.getvalue();
					}
					if (o instanceof EndPage) {
						EndPage endPage = (EndPage) o;
						end = endPage.getvalue();
					}
					if (o instanceof MedlinePgn) {
						MedlinePgn medlinePgn = (MedlinePgn) o;
						fullPagination.append(medlinePgn.getvalue());
					}
				}
				if (start != null && !start.isEmpty()) {
					fullPagination.append(start);
				}
				if (!Strings.isNullOrEmpty(end)) {
					fullPagination.append("-").append(end);
				}
			}
		}
		document.setPagination(fullPagination.toString());

		String pmid = medlineCitation.getPMID().getvalue();
		
		document.setPmid(pmid);
		
		Journal journal = article.getJournal();

		document.setJournalTitle(journal.getTitle());
		document.setJournalIso(journal.getISOAbbreviation());

		JournalIssue journalIssue = journal.getJournalIssue();
		
		String title = medlineCitation.getArticle().getArticleTitle();

		if (title != null) {
			document.setTitle(title);
		}


		Abstract articleAbstract = article.getAbstract();
		if (articleAbstract != null) {
			StringBuilder text = new StringBuilder();
			for (AbstractText abstractText : articleAbstract.getAbstractText()) {
				text.append(abstractText.getvalue());
				text.append(" ");
			}
			document.setAbstractText(text.toString().trim());
		}
		
		MedlineJournalInfo medlineJournalInfo = medlineCitation.getMedlineJournalInfo();
		if (medlineJournalInfo != null) {
			String issn = medlineJournalInfo.getISSNLinking();
			if (issn != null) {
				document.setIssn(issn);
			}
			
			String country = medlineJournalInfo.getCountry();
			if (country != null && !country.isEmpty()) {
				document.setJournalCountry(country);
			}
		}
		
		if (journalIssue != null) {
			String issue = journalIssue.getIssue();
			if (issue != null) {
				document.setJournalIssue(issue);
			}
			String volume = journalIssue.getVolume();
			if (volume != null) {
				document.setJournalVolume(volume);
			}


			
			PubDate pubDate = journalIssue.getPubDate();
			if (pubDate != null) {
				String year = null;
				String month = null;
				String day = null;
				// String season = null;
				// String medlineDate = null;
				
				for (Object o : pubDate.getYearOrMonthOrDayOrSeasonOrMedlineDate()) {
					if (o instanceof Year) {
						Year y = (Year) o;
						year = y.getvalue();
					}
					else if (o instanceof Month) {
						Month m = (Month) o;
						month = m.getvalue();
					}
					else if (o instanceof Day) {
						Day d = (Day) o;
						day = d.getvalue();
					}
					
					// else if (o instanceof Season) {
					// Season s = (Season) o;
					// season = s.getvalue();
					// }
					// else if (o instanceof MedlineDate) {
					// MedlineDate m = (MedlineDate) o;
					// medlineDate = m.getvalue();
					// }
				}
				Date d = null;
				if (year != null) {
					document.setPubYear(year);

					int yearInt = Integer.parseInt(year);

					DateTime dateTime = new DateTime().withYear(yearInt);
					
					if (month != null) {
						dateTime = dateTime.withMonthOfYear(shortMonths.indexOf(month) + 1);
					}
					else {
						dateTime = dateTime.withMonthOfYear(1);
					}
					
					if (day != null) {
						dateTime = dateTime.withDayOfMonth(Integer.parseInt(day));
					}
					else {
						dateTime = dateTime.withDayOfMonth(1);
					}
					
					d = dateTime.withTimeAtStartOfDay().toDate();
				}
				
				if (d != null) {
					document.setPublicationDate(d);
				}
				
			}
		}

		String citation = "";
		if (document.getAuthors() != null) {
			citation = joiner.join(document.getAuthors()) + ". ";
		}
		if (document.getTitle() != null) {
			citation = citation + title + ". ";
		}
		if (document.getJournalIso() != null) {
			citation = citation + document.getJournalIso() + ". ";
		}
		if (document.getPubYear() != null) {
			citation = citation + document.getPubYear() + ". ";
		}
		if (document.getJournalVolume() != null) {
			citation = citation + document.getJournalVolume() + " ";
		}

		if (document.getJournalIssue() != null) {
			citation = citation + "(" + document.getJournalVolume() + ") ";
		}
		if (document.getPagination() != null) {
			citation = citation + ":" + document.getPagination() + " ";
		}
		citation = citation.trim();
		document.setCitation(citation);


		
		return document;
		
	}
}