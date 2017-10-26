package gr.ilsp.fmc.utils;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.util.Version;


public class AnalyzerFactory {

	String[] langs = {"de", "el", "en", "es", "fr", "it","ru",};
	List<String> langsList = Arrays.asList(langs);

	public Analyzer getAnalyzer (String lang) throws Exception {
		if (lang.equals("de")) {
			return new GermanAnalyzer(Version.LUCENE_40);
		} else	if (lang.equals("el")) {
			return new GreekAnalyzer(Version.LUCENE_40);
		} else	if (lang.equals("en")) {
			return new EnglishAnalyzer(Version.LUCENE_40);
		} else if (lang.equals("es")) {
			return new SpanishAnalyzer(Version.LUCENE_40);
		} else	if (lang.equals("fr")) {
			return new FrenchAnalyzer(Version.LUCENE_40);
		} else	if (lang.equals("it")) {
			return new ItalianAnalyzer(Version.LUCENE_40);
		} else if (lang.equals("ru")) {
			return new RussianAnalyzer(Version.LUCENE_40);
		}
		else {
			throw new Exception("No analyzer available for language " + lang + ".\n"
					+ "Available languages are " + langsList.toString() + ".\n");
		}
	}
}
