/*
 * Copyright (c) 2010 TransPac Software, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package gr.ilsp.fmc.exporter;

import gr.ilsp.fmc.datums.ClassifierDatum;
import gr.ilsp.fmc.datums.CrawlDbDatum;
import gr.ilsp.fmc.datums.ExtendedParsedDatum;
import gr.ilsp.fmc.main.SimpleCrawlHFS;
import gr.ilsp.fmc.utils.ContentNormalizer;
import gr.ilsp.fmc.utils.CrawlConfig;
import gr.ilsp.fmc.utils.LatvianAnalyzer;
import gr.ilsp.fmc.utils.LithuanianAnalyzer;
import gr.ilsp.fmc.utils.PrettyPrintHandler;
import gr.ilsp.fmc.utils.TopicTools;
import gr.ilsp.fmc.utils.AnalyzerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Proxy;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.tika.language.LanguageIdentifier;
import org.codehaus.stax2.XMLOutputFactory2;
import org.codehaus.stax2.XMLStreamWriter2;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;


import bixo.datum.FetchedDatum;
import bixo.datum.UrlStatus;
import bixo.utils.CrawlDirUtils;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

@SuppressWarnings("deprecation")
public class SampleExporter {
	private static final Logger LOGGER = Logger.getLogger(SampleExporter.class);
	private static final int minTokensNumber=10;
	private static final String VAR_RES_CACHE = "/var/lib/tomcat6/webapps/soaplab2-results/";
	private static final String HTTP_PATH = "http://nlp.ilsp.gr/soaplab2-results/";	
	private static final String cesDocVersion = "0.4";

	private static int MIN_TOKENS_PER_PARAGRAPH;
	private static String crawlDirName;
	private static String language;
	private static String topic;
	private static boolean keepBoiler;
	private static String negWordsFile;
	private static String outputDir="";
	private static boolean textExport = false;
	private static SampleExporterOptions options = null;
	static Analyzer analyzer = null;
	static AnalyzerFactory analyzerFactory = new AnalyzerFactory();
	private static ArrayList<String> topicTermsAll = null;
	private static ArrayList<String> xmlFiles = new ArrayList<String>();
	private static String outputFile = null;

	
	private static void processStatus(JobConf conf, Path curDirPath) throws IOException {
		Path statusPath = new Path(curDirPath, CrawlConfig.STATUS_SUBDIR_NAME);
		Tap statusTap = new Hfs(new TextLine(), statusPath.toUri().toString());

		TupleEntryIterator iter = statusTap.openForRead(conf);

		UrlStatus[] statusValues = UrlStatus.values();
		int[] statusCounts = new int[statusValues.length];
		int totalEntries = 0;
		while (iter.hasNext()) {
			TupleEntry entry = iter.next();
			totalEntries += 1;

			// STATUS_FN, HEADERS_FN, EXCEPTION_FN, STATUS_TIME_FN, HOST_ADDRESS_FN).append(getSuperFields(StatusDatum.class)
			String statusLine = entry.getString("line");
			String[] pieces = statusLine.split("\t");
			UrlStatus status = UrlStatus.valueOf(pieces[0]);
			statusCounts[status.ordinal()] += 1;
		}


		for (int i = 0; i < statusCounts.length; i++) {
			if (statusCounts[i] != 0) {
				LOGGER.info(String.format("Status %s: %d", statusValues[i].toString(), statusCounts[i]));
			}
		}
		LOGGER.info("Total status: " + totalEntries);
		LOGGER.info("");
	}

	private static void processCrawlDb(JobConf conf, Path curDirPath, boolean exportDb) throws IOException {
		TupleEntryIterator iter;
		int totalEntries;
		Path crawlDbPath = new Path(curDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
		Tap crawldbTap = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toUri().toString());
		iter = crawldbTap.openForRead(conf);
		totalEntries = 0;
		int fetchedUrls = 0;
		int unfetchedUrls = 0;
		/*LOGGER.info("!!!! PRINTING CRAWLDB !!!!");
		while (iter.hasNext()) {
			TupleEntry entry = iter.next();
			totalEntries += 1;			
			CrawlDbDatum datum = new CrawlDbDatum(entry);
			if (exportDb) {
				LOGGER.info(datum.toString());
			}
			if (datum.getLastFetched() == 0) {
				unfetchedUrls += 1;
			} else {
				fetchedUrls += 1;
			}
		}*/
		LOGGER.info("!!!! PRINTING CLASSIFIED !!!!");
		int prevLoop = -1;
		Path crawlDirPath = curDirPath.getParent();
		FileSystem fs = crawlDirPath.getFileSystem(conf);
		while ((curDirPath = CrawlDirUtils.findNextLoopDir(fs, crawlDirPath, prevLoop)) != null) {
			int curLoop = CrawlDirUtils.extractLoopNumber(curDirPath);
			if (curLoop != prevLoop + 1) {
				LOGGER.warn(String.format("Missing directories between %d and %d", prevLoop, curLoop));
			}

			Path classifiedPath = new Path(curDirPath, CrawlConfig.CLASSIFIER_SUBDIR_NAME);
			Tap classifiedTap = new Hfs(new SequenceFile(ClassifierDatum.FIELDS),classifiedPath.toUri().toString());
			iter = classifiedTap.openForRead(conf);
			while (iter.hasNext()) {
				TupleEntry entry = iter.next();
				ClassifierDatum datum = new ClassifierDatum(entry);
				LOGGER.info(datum.toString());
			}


			prevLoop = curLoop;
		}



		if (!exportDb) {
			LOGGER.info(String.format("%d fetched URLs", fetchedUrls));
			LOGGER.info(String.format("%d unfetched URLs", unfetchedUrls));
			LOGGER.info("Total URLs: " + totalEntries);
			LOGGER.info("");
		}
	}



	public void export(boolean loadProfile) {

		long start = System.currentTimeMillis();
		try {
			JobConf conf = new JobConf();
			Path crawlDirPath = new Path(crawlDirName);
			FileSystem fs = crawlDirPath.getFileSystem(conf);

			if (!fs.exists(crawlDirPath)) {
				System.err.println("Prior crawl output directory does not exist: " + crawlDirName);
				System.exit(-1);
			}

			//LanguageIdentifier initialization	
			if (loadProfile){
				String path = "profs/";
				InputStream is = null;
				URL urldir = SimpleCrawlHFS.class.getResource("/profs");
				if (urldir.getProtocol()=="jar"){
					String jarPath = urldir.getPath().substring(5, urldir.getPath().indexOf("!")); //strip out only the JAR file
					JarFile jar = new JarFile(jarPath);
					Enumeration<JarEntry> entries = jar.entries(); //gives ALL entries in jar
					List<String> profs = new ArrayList<String>();
					while(entries.hasMoreElements()) {
						String name = entries.nextElement().getName();
						if (name.startsWith("profs/")) { //filter according to the path
							String entry = name.substring(path.length());
							int checkSubdir = entry.indexOf("/");
							if (checkSubdir < 0) {
								is = Detector.class.getResourceAsStream("/profs/"+entry);
								profs.add(TopicTools.convertStreamToString(is));
								is.close();
							}	              
						}
					}
					try {
						DetectorFactory.loadProfile(profs);				
					} catch (LangDetectException e1) {
						LOGGER.error(e1.getMessage());
					} 
				} else {
					try {
						DetectorFactory.loadProfile(new File(urldir.toURI()));
					} catch (LangDetectException e) {
						LOGGER.error(e.getMessage());
					} catch (URISyntaxException e) {
						LOGGER.error(e.getMessage());
					}
				}
			}

			// Skip Hadoop/Cascading DEBUG messages.
			Logger.getRootLogger().setLevel(Level.INFO);
			boolean exportAllXmls = true;

			if (exportAllXmls) {
				int prevLoop = -1;
				Path curDirPath = null;
				int id = 1;
				//Path xmlPath = new Path(crawlDirPath,CrawlConfig.XML_SUBDIR_NAME);						
				//if (fs.exists(xmlPath))
				//	fs.delete(xmlPath);
				//fs.mkdirs(xmlPath);
				String topicFile = getTopic();
				ArrayList<String[]> topic = null;
				if (topicFile!=null) {
					topic=TopicTools.analyzeTopic(topicFile,language);
					topicTermsAll = TopicTools.analyzeTopicALL(topicFile);
				}
				while ((curDirPath = CrawlDirUtils.findNextLoopDir(fs, crawlDirPath, prevLoop)) != null) {
					id = exportToXml(conf,curDirPath,language, id,topic);
					int curLoop = CrawlDirUtils.extractLoopNumber(curDirPath);
					if (curLoop != prevLoop + 1) {
						LOGGER.warn(String.format("Missing directories between %d and %d", prevLoop, curLoop));
					}

					prevLoop = curLoop;
				}
				LOGGER.info("Completed in " + (System.currentTimeMillis()-start) + " milliseconds.");

				OutputStreamWriter xmlFileListWrt;
				xmlFileListWrt = new OutputStreamWriter(new FileOutputStream(outputFile),"UTF-8");
				for (String xmlFile: xmlFiles) {
					xmlFileListWrt.write(xmlFile.replace(VAR_RES_CACHE, HTTP_PATH).replace("file:", "")   +"\n");
				}
				xmlFileListWrt.close();
				return;
				//System.exit(0);
			}

			boolean exportDb = true;
			if (exportDb) {
				Path latestCrawlDirPath = CrawlDirUtils.findLatestLoopDir(fs, crawlDirPath);
				processCrawlDb(conf, latestCrawlDirPath, exportDb);	
				//exportToXml(conf, latestCrawlDirPath, "el");
			} else {
				int prevLoop = -1;
				Path curDirPath = null;
				while ((curDirPath = CrawlDirUtils.findNextLoopDir(fs, crawlDirPath, prevLoop)) != null) {
					String curDirName = curDirPath.toUri().toString();
					LOGGER.info("");
					LOGGER.info("================================================================");
					LOGGER.info("Processing " + curDirName);
					LOGGER.info("================================================================");

					int curLoop = CrawlDirUtils.extractLoopNumber(curDirPath);
					if (curLoop != prevLoop + 1) {
						LOGGER.warn(String.format("Missing directories between %d and %d", prevLoop, curLoop));
					}

					prevLoop = curLoop;

					// Process the status and crawldb in curPath
					processStatus(conf, curDirPath);
					processCrawlDb(conf, curDirPath, exportDb);

				}
			}
		} catch (Throwable t) {
			LOGGER.error("Exception running tool", t);
			System.exit(-1);
		}

	}

	public static void main(String[] args) {
		SampleExporter se = new SampleExporter();
		options = new SampleExporterOptions();
		options.parseOptions(args);
		se.setMIN_TOKENS_PER_PARAGRAPH(options.get_length());
		se.setCrawlDirName (options.get_inputdir());
		se.setOutputFile(options.get_inputdir() + System.getProperty("file.separator") + "outputlist.txt");					
		if (options.get_topic()!=null) {
			se.setTopic(options.get_topic());
		}
		se.setLanguage(options.get_language());
		if (options.get_topic()!=null) {
			se.setTopic(options.get_topic());
		}
		if (options.get_negwords()!=null) {
			se.setNegWordsFile(options.get_negwords());
		}
		if (options.get_outputdir()!=null) {
			se.setOutputDir(options.get_outputdir());
		}
		if (options.get_textexport()) {
			se.setTextExport(true);
		}
		se.export(true);

	}

	private static int exportToXml(JobConf conf, Path curDirPath, String language, int id, ArrayList<String[]> topic) throws IOException {
		TupleEntryIterator iter;
		String title = "";
		String cleanText = "";
		String htmlText = "";
		String domain = "";
		String format = "";	
		String subdomains = "";
		String contentEncoding = "";
		ArrayList<String> terms = null;
		String url = "";
		Map<String,String> meta = null;
		//LOGGER.setLevel(Level.DEBUG);
		Path parseDbPath = new Path(curDirPath, CrawlConfig.PARSE_SUBDIR_NAME);
		Tap parseDbTap = new Hfs(new SequenceFile(ExtendedParsedDatum.FIELDS), parseDbPath.toUri().toString());
		Path contentPath = new Path(curDirPath,CrawlConfig.CONTENT_SUBDIR_NAME);
		Tap contentDbTap = new Hfs(new SequenceFile(FetchedDatum.FIELDS), contentPath.toUri().toString());
		Path classifierPath = new Path(curDirPath, CrawlConfig.CLASSIFIER_SUBDIR_NAME);
		Tap classifierDbTap = new Hfs(new SequenceFile(ClassifierDatum.FIELDS), classifierPath.toUri().toString());
		TupleEntryIterator classIter = classifierDbTap.openForRead(conf);
		
		Path xmlPath = null;
		if (outputDir.length()==0)
			xmlPath = new Path(curDirPath.getParent(), CrawlConfig.XML_SUBDIR_NAME);
		else 
			xmlPath = new Path(outputDir);
		FileSystem fs = xmlPath.getFileSystem(conf);
		if (!fs.exists(xmlPath)) fs.mkdirs(xmlPath);
		TupleEntryIterator contentIter = contentDbTap.openForRead(conf);
		iter = parseDbTap.openForRead(conf);
		
		//String topicFile = options.get_topic();		
		//ArrayList<String[]> topic = null;
		//if (topicFile!=null)
		//	topic=TopicTools.analyzeTopic(topicFile,language);
		String[] neg_words = null ;
		if (getNegWordsFile() != null) {
			String neg_words_filename = getNegWordsFile();
			neg_words = getForbiddenwords(neg_words_filename);
		}
		TupleEntry entry = null;
		
		ExtendedParsedDatum datum = null;
		while (iter.hasNext()) {
			entry = iter.next();
			datum = new ExtendedParsedDatum(entry);
			url = datum.getUrl();
			title = datum.getTitle();
			if (title==null) title = "";
			cleanText = datum.getParsedText();
			cleanText = ContentNormalizer.normalizeText(cleanText);
			meta = datum.getParsedMeta();
			contentEncoding = meta.get("Content-Encoding");
			htmlText = getHtml(url,curDirPath,contentIter, contentEncoding);
			subdomains = getSubdomains(url, curDirPath,classIter);
			datum = null;
			entry = null;
			LOGGER.debug("Writing: " + id + " " + url);
			String termsArray = meta.get("keywords");
			terms = new ArrayList<String>();
			if (termsArray!=null){
				termsArray = termsArray.replace(",","");
				for (String s: termsArray.split(" "))
					terms.add(s);
			}
			format = meta.get("Content-Type");	
			format = validFormat(format);			
			if (XMLExporter(xmlPath,format, title, url, language, htmlText, cleanText,id, "", domain, subdomains, terms, topic, neg_words ))
				id++;
			if (textExport) TextExporter(xmlPath,cleanText,id-1);						
		}
		iter.close();
		classIter.close();
		contentIter.close();
		return id;
	}

	private static String getHtml(String url, Path curDirPath, TupleEntryIterator contentIter, String contentEncoding){
		String htmltext = "";
		BufferedReader reader = null;
		while (contentIter.hasNext()){
			TupleEntry entry = contentIter.next();
			FetchedDatum datum = new FetchedDatum(entry);

			if (datum.getUrl().equals(url)) {
				InputStream is = new ByteArrayInputStream(datum.getContentBytes(), 0, datum.getContentLength());
				try {
					reader = new BufferedReader(new InputStreamReader(is,contentEncoding));
					String line = "";
					while ((line=reader.readLine())!=null)
						htmltext=htmltext.concat(line + "\r\n");
					reader.close();					
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally { 
					try {		
						is.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				entry = null;
				datum = null;
				return htmltext;
			}
		}		
		return null;
	}

	private static String getSubdomains(String url, Path curDirPath, TupleEntryIterator contentIter){
		String subdomains = "";

		while (contentIter.hasNext()){
			TupleEntry entry = contentIter.next();
			ClassifierDatum datum = new ClassifierDatum(entry);

			if (datum.getUrl().equals(url)) {
				String[] subclasses = datum.getSubClasses();
				//Double[][] subscores = datum.getSubScores();
				//int count = 0;
				for (String s: subclasses){
					subdomains=subdomains.concat(s + ";");
					//LOGGER.info(url + " " + s + " " + subscores[count][0]);
					//count++;
				}
				entry = null;
				datum = null;
				if (subdomains == "") return subdomains;
				return subdomains.substring(0,subdomains.length()-1);
			}
		}		
		//if (subdomains.length()>0) subdomains = subdomains.substring(0,subdomains.length()-2);
		return null;
	}

	private static String validFormat(String format){
		String result = format;
		if (format.contains(";")){
			result = format.split(";")[0];
		}
		return result;
	}

	public static String[] getForbiddenwords(String filename){
		File words_file = new File(filename);

		if (words_file.exists()){
			ArrayList<String> words1 = new ArrayList<String>();
			try {
				BufferedReader in = new BufferedReader(new FileReader(words_file));
				String str; int count=0;
				while ((str = in.readLine()) != null) {
					words1.add(str);
					count++; 
				}
				in.close();
				String[] words = new String[count];
				System.arraycopy(words1.toArray(), 0, words, 0, count);
				return words;
			} catch (IOException e) {
				System.err.println("Problem in reading the file with the forbidden words");
				//String[] words = null;
				return null;
			}
		}else{
			return null;
		}
	}

	public static Boolean XMLExporter_OR(Path outputdir, String format, String title, String eAddress,
			String lang, String html_text, String cleaned_text, int id, String pubDate, String domain, String subdomain,
			ArrayList<String> terms, ArrayList<String[]> topic, String[] neg_words) { //throws Exception {
		StringTokenizer tkzr = new StringTokenizer(cleaned_text);
		if (tkzr.countTokens()<minTokensNumber){
			return false;		
		}

		//Filename of files to be written.
		String temp_id=Integer.toString(id);
		String html_filename = temp_id+".html";
		Path xml_file = new Path(outputdir,temp_id+".xml");
		Path annotation = new Path(outputdir,html_filename);
		OutputStreamWriter tmpwrt;
		try {
			tmpwrt = new OutputStreamWriter(new FileOutputStream(annotation.toUri().getPath()),"UTF-8");
			tmpwrt.write(html_text);
			tmpwrt.close();
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			//throw new Exception("Problem in encoding during writing HTML");
		} catch (IOException e) {
			e.printStackTrace();
			//throw new Exception("Problem in encoding during writing HTML");
		}


		//Write the XML file
		int parId = 1;

		XMLOutputFactory2 xof = (XMLOutputFactory2) XMLOutputFactory2.newInstance();
		XMLStreamWriter2 xtw1 = null;
		XMLStreamWriter2 xtw = null;
		OutputStreamWriter wrt = null;

		try {
			wrt = new OutputStreamWriter(new FileOutputStream(xml_file.toUri().getPath()),"UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			xtw1 = (XMLStreamWriter2)
			xof.createXMLStreamWriter(wrt);
			PrettyPrintHandler handler = new PrettyPrintHandler(
					xtw1 );
			xtw = (XMLStreamWriter2)
			Proxy.newProxyInstance(XMLStreamWriter2.class.getClassLoader(),
					new Class[] { XMLStreamWriter2.class }, handler);
			xtw.writeStartDocument();
			xtw.writeStartElement("cesDoc");
			xtw.writeAttribute("version", "0.4");
			xtw.writeAttribute("xmlns",
			"http://www.xces.org/schema/2003");
			xtw.writeAttribute("xmlns:xlink",
			"http://www.w3.org/1999/xlink");
			xtw.writeAttribute("xmlns:xsi",
			"http://www.w3.org/2001/XMLSchema-instance");
			createHeader(xtw, eAddress, pubDate, lang, title, domain, terms, annotation.toUri().getPath(), format, subdomain);
			//System.err.println("Now working on file:+fileNo);
			xtw.writeStartElement("text");
			xtw.writeStartElement("body");
			try {

				String lines[] = cleaned_text.split("\n");
				for (String line : lines ) {
					if (line.length()==0) continue;
					if (line.toCharArray()[0]==160)continue;

					//if (line.substring(0, 8).equals("<boiler>")) {
					if (line.indexOf("<boiler>")==0){
						line = line.substring(8, line.length()-9);
						if (line.trim().length()<=1) continue;						
						xtw.writeStartElement("p");
						xtw.writeAttribute("id",("p"+parId));
						xtw.writeAttribute("type","boilerplate");
					}else {
						//if (line.substring(0, 6).equals("<text>"))
						if (line.indexOf("<text>")==0)
							line = line.substring(6, line.length()-7);													
						if (line.trim().length()<=1) continue;
						xtw.writeStartElement("p");
						xtw.writeAttribute("id",("p"+parId));

						if (!countWords(line, MIN_TOKENS_PER_PARAGRAPH )) {
							xtw.writeAttribute("type","length");
						}
						else {
							String langidentified ="";
							LanguageIdentifier LangI=new LanguageIdentifier(line); 							
							langidentified = LangI.getLanguage();
							if (!langidentified.equals(lang)){
								//not in the right language
								xtw.writeAttribute("type", "lang");
							} else {
								if (findWords(line,neg_words)){	//contain "forbidden" words
									xtw.writeAttribute("type", "content");
								}
								else {
									//does the paragraph contain terms?
									String[] tempstr = new String[1];		
									String term;
									ArrayList<String> stems =new ArrayList<String>();
									try {
										stems = TopicTools.analyze(line, langidentified);
									} catch (IOException e) {
										e.printStackTrace();
									} 
									String par_text="";
									for (String st:stems){
										par_text=par_text.concat(" "+st);
									}
									par_text = par_text.trim();
									Boolean found = false;
									if (topic!=null) {
										for (int ii=0;ii<topic.size();ii++){ //for each row of the topic
											tempstr=topic.get(ii);
											term = tempstr[1];
											Pattern pattern = Pattern.compile(" "+term+" ");	
											Matcher matcher = pattern.matcher(" "+par_text+" ");
											if (matcher.find()){
												found=true;
												break;
											}
										}
										if (!found){//does not contain terms
											xtw.writeAttribute("type","terms");
										}
									}
								}
							}
						}
					}
					xtw.writeCharacters(line);
					xtw.writeEndElement();
					parId++;
				}							
			} catch (Exception e) {
				LOGGER.error("Could not write file with id " + temp_id);	
				LOGGER.error(e.getMessage());
				e.printStackTrace();
				return false;
			}
			xtw.writeEndElement();
			xtw.writeEndElement();
			xtw.writeEndElement();
			xtw.flush();							
		} catch (XMLStreamException e) {
			LOGGER.error("Could not write XML " + xml_file);
			LOGGER.error(e.getMessage());
			return false;			
		} finally {
			try {				
				xtw.close();
				xtw1.close();
				wrt.close();
			} catch (XMLStreamException e) {
				LOGGER.error(e.getMessage());
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
			}
		}

		return true;
	}

	public static void TextExporter(Path outpath, String text, int id){
		Path txt_file = new Path(outpath,language + "_" + id + ".txt");
		try {
			BufferedWriter wrt = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(txt_file.toUri().getPath()),"UTF-8"));
			text = text.replaceAll("<boiler>.*</boiler>\r\n", "");
			text = text.replaceAll("<[^<]*>", "");
			wrt.write(text);
			wrt.close();			
		} catch (UnsupportedEncodingException e) {
			LOGGER.error(e.getMessage());			
		} catch (FileNotFoundException e) {
			LOGGER.error(e.getMessage());
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
	}

	public static Boolean XMLExporter(Path outputdir, String format, String title, String eAddress,
			String lang, String html_text, String cleaned_text, int id, String pubDate, String domain, String subdomain,
			ArrayList<String> terms, ArrayList<String[]> topic, String[] neg_words) { //throws Exception {
		StringTokenizer tkzr = new StringTokenizer(cleaned_text);
		boolean xmlwritten = false;
		if (tkzr.countTokens()<minTokensNumber){
			return false;		
		}
		String foundt ="";
		String langidentified ="";
		if (!keepBoiler){
			cleaned_text = cleaned_text.replaceAll("<boiler.*</boiler>\n", "");
		}
		//Filename of files to be written.
		String temp_id=lang + "_" + Integer.toString(id);
		String html_filename = temp_id+".html";
		Path xml_file = new Path(outputdir,temp_id+".xml");
		Path annotation = new Path(outputdir,html_filename);
		
		OutputStreamWriter tmpwrt;
		try {
			tmpwrt = new OutputStreamWriter(new FileOutputStream(annotation.toUri().getPath()),"UTF-8");
			tmpwrt.write(html_text);
			tmpwrt.close();
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			//throw new Exception("Problem in encoding during writing HTML");
		} catch (IOException e) {
			e.printStackTrace();
			//throw new Exception("Problem in encoding during writing HTML");
		}


		//Write the XML file
		int parId = 1;

		XMLOutputFactory2 xof = (XMLOutputFactory2) XMLOutputFactory2.newInstance();
		XMLStreamWriter2 xtw1 = null;
		XMLStreamWriter2 xtw = null;
		OutputStreamWriter wrt = null;

		try {
			wrt = new OutputStreamWriter(new FileOutputStream(xml_file.toUri().getPath()),"UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return false;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		try {
			xtw1 = (XMLStreamWriter2)
			xof.createXMLStreamWriter(wrt);
			PrettyPrintHandler handler = new PrettyPrintHandler(
					xtw1 );
			xtw = (XMLStreamWriter2)
			Proxy.newProxyInstance(XMLStreamWriter2.class.getClassLoader(),
					new Class[] { XMLStreamWriter2.class }, handler);
			xtw.writeStartDocument();
			xtw.writeStartElement("cesDoc");
			xtw.writeAttribute("version", "0.4");
			xtw.writeAttribute("xmlns",
			"http://www.xces.org/schema/2003");
			xtw.writeAttribute("xmlns:xlink",
			"http://www.w3.org/1999/xlink");
			xtw.writeAttribute("xmlns:xsi",
			"http://www.w3.org/2001/XMLSchema-instance");
			createHeader(xtw, eAddress, pubDate, lang, title, domain, terms, annotation.toUri().getPath(), format, subdomain);
			//System.err.println("Now working on file:+fileNo);
			xtw.writeStartElement("text");
			xtw.writeStartElement("body");
			

				String lines[] = cleaned_text.split("\n");
				for (String line : lines ) {
					if (line.length()==0) continue;
					if (line.toCharArray()[0]==160)continue;
					xtw.writeStartElement("p");
					xtw.writeAttribute("id", ("p"+parId));					
					//if (line.substring(0,7).equals("<boiler")) {					
					if (line.indexOf("<boiler")==0) {
						if (line.substring(0, 8).equals("<boiler>")) {
							xtw.writeAttribute("crawlinfo","boilerplate");
							line = line.substring(8, line.length()-9);
						}
						else if (line.substring(0, 15).equals("<boiler type='t")) {
							xtw.writeAttribute("crawlinfo","boilerplate");
							xtw.writeAttribute("type","title");
							line = line.substring(21, line.length()-9);
						}
						else if (line.substring(0, 15).equals("<boiler type='h")) {
							xtw.writeAttribute("crawlinfo","boilerplate");
							xtw.writeAttribute("type","heading");
							line = line.substring(23, line.length()-9);
						}
						else if (line.substring(0, 15).equals("<boiler type='l")) {
							xtw.writeAttribute("crawlinfo","boilerplate");
							xtw.writeAttribute("type","listitem");
							line = line.substring(24, line.length()-9);
						}
					}
					//else if (line.substring(0, 5).equals("<text")){
					else if (line.indexOf("<text")==0){
						if (line.substring(0,6).equals("<text>")) {
							line = line.substring(6, line.length()-7);
							if (!countWords(line,MIN_TOKENS_PER_PARAGRAPH))
								xtw.writeAttribute("crawlinfo", "ooi-length");
							else if (!lang.isEmpty()){
								langidentified = checkLang(line);
								if (!langidentified.equals(lang))
									xtw.writeAttribute("crawlinfo", "ooi-lang");
								else if (findWords(line, neg_words))
									xtw.writeAttribute("crawlinfo", "ooi-neg");
								else {
									foundt = findTopicTerms(line, topic, lang, topicTermsAll);
									//xtw.writeAttribute("crawlinfo","text");
									//xtw.writeAttribute("type","text");
									if (!foundt.isEmpty())
										xtw.writeAttribute("topic", foundt);
								}
							}
						}
						else if (line.substring(0,13).equals("<text type='t")) {
							line = line.substring(19, line.length()-7);
							if (!countWords(line,MIN_TOKENS_PER_PARAGRAPH)){
								xtw.writeAttribute("crawlinfo", "ooi-length");
								xtw.writeAttribute("type","title");
							}
							else if (!lang.isEmpty()){
								langidentified = checkLang(line);
								if (!langidentified.equals(lang)){
									xtw.writeAttribute("crawlinfo", "ooi-lang");
									xtw.writeAttribute("type","title");
								}
								else if (findWords(line, neg_words)){
									xtw.writeAttribute("crawlinfo", "ooi-neg");
									xtw.writeAttribute("type","title");
								}
								else {
									foundt = findTopicTerms(line, topic, lang, topicTermsAll);
									//xtw.writeAttribute("crawlinfo","text");
									xtw.writeAttribute("type","title");
									if (!foundt.isEmpty()){
										xtw.writeAttribute("topic", foundt);
									}
								}
							}
						}
						else if (line.substring(0,13).equals("<text type='l")) {
							line = line.substring(22, line.length()-7);
							if (!countWords(line,MIN_TOKENS_PER_PARAGRAPH)){
								xtw.writeAttribute("crawlinfo", "ooi-length");
								xtw.writeAttribute("type","listitem");
							}
							else if (!lang.isEmpty()){
								langidentified = checkLang(line);
								if (!langidentified.equals(lang)){
									xtw.writeAttribute("crawlinfo", "ooi-lang");
									xtw.writeAttribute("type","listitem");
								}
								else if (findWords(line, neg_words)){
									xtw.writeAttribute("crawlinfo", "ooi-neg");
									xtw.writeAttribute("type","listitem");
								}
								else {
									foundt = findTopicTerms(line, topic, lang, topicTermsAll);
									//xtw.writeAttribute("crawlinfo","text");
									xtw.writeAttribute("type","listitem");
									if (!foundt.isEmpty()){
										xtw.writeAttribute("topic", foundt);
									}
								}
							}
						}
						else if (line.substring(0,13).equals("<text type='h")) {
							line = line.substring(21, line.length()-7);
							if (!countWords(line,MIN_TOKENS_PER_PARAGRAPH)){
								xtw.writeAttribute("crawlinfo", "ooi-length");
								xtw.writeAttribute("type","heading");
							}
							else if (!lang.isEmpty()){
								langidentified = checkLang(line);
								if (!langidentified.equals(lang)){
									xtw.writeAttribute("crawlinfo", "ooi-lang");
									xtw.writeAttribute("type","heading");
								}
								else if (findWords(line, neg_words)){
									xtw.writeAttribute("crawlinfo", "ooi-neg");
									xtw.writeAttribute("type","heading");
								}
								else {
									foundt = findTopicTerms(line, topic, lang, topicTermsAll);
									//xtw.writeAttribute("crawlinfo","text");
									xtw.writeAttribute("type","heading");
									if (!foundt.isEmpty()){
										xtw.writeAttribute("topic", foundt);
									}
								}
							}
						}
					}
					else {
						//if (line.substring(0, 6).equals("<text>"))
						//if (line.indexOf("<text>")==0)
						//	line = line.substring(6, line.length()-7);													
						if (line.trim().length()<=1) continue;
						//xtw.writeStartElement("p");
						//xtw.writeAttribute("id",("p"+parId));

						if (!countWords(line, MIN_TOKENS_PER_PARAGRAPH )) {
							xtw.writeAttribute("type","length");
						}
						else {
							//String langidentified ="";
							LanguageIdentifier LangI=new LanguageIdentifier(line); 							
							langidentified = LangI.getLanguage();
							if (!langidentified.equals(lang)){
								//not in the right language
								xtw.writeAttribute("type", "lang");
							} else {
								if (findWords(line,neg_words)){	//contain "forbidden" words
									xtw.writeAttribute("type", "content");
								}
								else {
									//does the paragraph contain terms?
									String[] tempstr = new String[1];		
									String term;
									ArrayList<String> stems =new ArrayList<String>();
									try {
										stems = TopicTools.analyze(line, langidentified);
									} catch (IOException e) {
										e.printStackTrace();
									} 
									String par_text="";
									for (String st:stems){
										par_text=par_text.concat(" "+st);
									}
									par_text = par_text.trim();
									Boolean found = false;
									if (topic!=null) {
										for (int ii=0;ii<topic.size();ii++){ //for each row of the topic
											tempstr=topic.get(ii);
											term = tempstr[1];
											Pattern pattern = Pattern.compile(" "+term+" ");	
											Matcher matcher = pattern.matcher(" "+par_text+" ");
											if (matcher.find()){
												found=true;
												break;
											}
										}
										if (!found){//does not contain terms
											xtw.writeAttribute("type","terms");
										}
									}
								}
							}
						}
					}
					xtw.writeCharacters(line);
					xtw.writeEndElement();
					parId++;
				}							
			
			xtw.writeEndElement();
			xtw.writeEndElement();
			xtw.writeEndElement();
			xtw.flush();
			xmlwritten = true;
		} catch (Exception e) {
			LOGGER.error("Could not write XML " + xml_file);
			xmlwritten = false;
			//LOGGER.error(e.getMessage());
			//return false;			
		} finally {
			try {			
				xof = null;
				if (xtw!=null)					
					xtw.close();
				if (xtw1!=null)
					xtw1.close();
				if (wrt!=null)
					wrt.close();
			} catch (XMLStreamException e) {
				LOGGER.error(e.getMessage());
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
			}
		}
		if (xmlwritten){
			xmlFiles.add(xml_file.toString());			
		}
		return xmlwritten;
	}

	private static String checkLang(String partOfLine) {
		String langidentified ="";
		if (partOfLine.length()<8) return langidentified;
		//LanguageIdentifier LangI=new LanguageIdentifier(partOfLine); 
		//langidentified = LangI.getLanguage();
		//if (!langidentified.equals(targetLang)){
		Detector detector = null;			
		try {
			detector = DetectorFactory.create();
			detector.append(partOfLine);
			langidentified = detector.detect();										
		} catch (LangDetectException e) {
			//LOGGER.error(e.getMessage());
		}
		//}
		return langidentified;
	}

	public static String findTopicTerms(String part_of_text,
			ArrayList<String[]> topic_terms, String lang, ArrayList<String> topic_termsALL){
		String found="";
		if (topic_terms==null || lang.isEmpty())
			return found;
		String[] tempstr = new String[1];		String term;
		ArrayList<String> stems =new ArrayList<String>();
		try {
			stems = analyze(part_of_text, lang);
		} catch (IOException e) {
			e.printStackTrace();
		} 
		String par_text="";
		for (String st:stems){
			par_text+=" "+st;
		}
		par_text = par_text.trim();
		for (int ii=0;ii<topic_terms.size();ii++){ //for each row of the topic
			tempstr=topic_terms.get(ii);
			term = tempstr[1];
			Pattern pattern = Pattern.compile(" "+term+" ");	
			Matcher matcher = pattern.matcher(" "+par_text+" ");
			if (matcher.find()){
				found=found+";"+topic_termsALL.get(ii); 
			}
		}
		if (!found.isEmpty())
			found=found.substring(1);
		return found;
	}

	public static ArrayList<String> analyze(String text, String lang) throws IOException  {
		ArrayList<String> stems = new ArrayList<String>();
		if (lang.equals("lv")) {
			stems = LatvianAnalyzer.analyze(text);
		} 
		else if (lang.equals("lt")){
			stems = LithuanianAnalyzer.analyze(text);
		}
		else{
			try {
				analyzer = analyzerFactory.getAnalyzer(lang);
			} catch (Exception e) {
				//logger.fatal("Cannot initialize analyzer for lang " + lang);
				e.printStackTrace();
				return null;
			}
			TokenStream tokenStream = analyzer.tokenStream("contents", new StringReader(text));
			CharTermAttribute termAtt = (CharTermAttribute) tokenStream.addAttribute(CharTermAttribute.class);
			while (tokenStream.incrementToken()) {
				stems.add(termAtt.toString());
			}
		}
		return stems;
	}


	public static Boolean countWords(String par_text, int thresh){
		StringTokenizer st = new StringTokenizer(par_text);
		int count=st.countTokens();
		Boolean type=true;
		//int count = 0;  String ss="";
		//while (st.hasMoreTokens()) {ss= st.nextToken();count++;}
		if (count<thresh){	
			type=false;
		}
		return type;		
	}

	public static Boolean findWords(String par_text, String[] words){
		if (words==null) 
			return false;
		Boolean type=false; int ind=0;
		for (int ii=0;ii<words.length;ii++){
			ind =par_text.indexOf(words[ii]); 
			if (ind>0){
				type=true;
				break;
			}
		}
		return type;		
	}

	private static void createHeader(XMLStreamWriter2 xtw, String url, String pubDate,
			String language, String title, String domain, ArrayList<String> terms, 
			String htmlFilename, String file_format, String subdomain) throws XMLStreamException {
		xtw.writeStartElement("cesHeader");
		xtw.writeAttribute("version", cesDocVersion);
		xtw.writeStartElement("fileDesc");
		xtw.writeStartElement("titleStmt");
		xtw.writeStartElement("title");
		xtw.writeCharacters(title.toString());
		xtw.writeEndElement();
		xtw.writeStartElement("respStmt");
		xtw.writeStartElement("resp");
		xtw.writeStartElement("type");
		xtw.writeCharacters("Crawling and normalization");
		xtw.writeEndElement();
		xtw.writeStartElement("name");
		xtw.writeCharacters("ILSP");
		xtw.writeEndElement();
		xtw.writeEndElement();
		xtw.writeEndElement();
		xtw.writeEndElement();
		xtw.writeStartElement("publicationStmt");   
		xtw.writeStartElement("distributor");
		//xtw.writeCharacters("Panacea project");
		xtw.writeCharacters("ACCURAT project");
		xtw.writeEndElement();
		xtw.writeStartElement("eAddress");
		xtw.writeAttribute("type", "web");
		//xtw.writeCharacters("http://www.panacea-lr.eu");
		xtw.writeCharacters("http://www.accurat-project.eu/");
		xtw.writeEndElement();
		xtw.writeStartElement("availability");
		xtw.writeCharacters("Under review");
		xtw.writeEndElement();
		xtw.writeStartElement("pubDate");
		xtw.writeCharacters("2012");
		xtw.writeEndElement();
		xtw.writeEndElement();

		xtw.writeStartElement("sourceDesc");
		xtw.writeStartElement("biblStruct");
		xtw.writeStartElement("monogr");
		xtw.writeStartElement("title");
		xtw.writeCharacters(title.toString());
		xtw.writeEndElement();
		xtw.writeStartElement("author");
		xtw.writeCharacters("");
		xtw.writeEndElement();
		xtw.writeStartElement("imprint");
		//new
		xtw.writeStartElement("format");
		xtw.writeCharacters(file_format);
		xtw.writeEndElement();
		//end of new
		xtw.writeStartElement("publisher");
		xtw.writeCharacters("");
		xtw.writeEndElement();
		xtw.writeStartElement("pubDate");
		xtw.writeCharacters(pubDate);
		xtw.writeEndElement();
		xtw.writeStartElement("eAddress");
		xtw.writeCharacters(url);
		xtw.writeEndElement();
		xtw.writeEndElement();

		xtw.writeEndElement();
		xtw.writeEndElement();
		xtw.writeEndElement();
		xtw.writeEndElement();
		xtw.writeStartElement("profileDesc");

		xtw.writeStartElement("langUsage");
		xtw.writeStartElement("language");
		xtw.writeAttribute("iso639", language!=null?language:"");
		xtw.writeEndElement();
		xtw.writeEndElement();

		xtw.writeStartElement("textClass");
		if (terms!=null){
			xtw.writeStartElement("keywords");
			for (String term:terms) {
				xtw.writeStartElement("keyTerm");
				xtw.writeCharacters(term);
				xtw.writeEndElement();
			}
			xtw.writeEndElement();
		}
		xtw.writeStartElement("domain");
		xtw.writeCharacters(domain);
		xtw.writeEndElement();
		xtw.writeStartElement("subdomain");
		xtw.writeCharacters(subdomain!=null?subdomain:"");
		xtw.writeEndElement();
		xtw.writeStartElement("subject");
		xtw.writeEndElement();
		xtw.writeEndElement();
		xtw.writeStartElement("annotations");
		xtw.writeStartElement("annotation");
		xtw.writeCharacters(htmlFilename.replace(VAR_RES_CACHE,
				HTTP_PATH));
		xtw.writeEndElement();
		xtw.writeEndElement();
		xtw.writeEndElement();
		xtw.writeEndElement();// cesHeader
	}




	public void setMIN_TOKENS_PER_PARAGRAPH(int mIN_TOKENS_PER_PARAGRAPH) {
		MIN_TOKENS_PER_PARAGRAPH = mIN_TOKENS_PER_PARAGRAPH;
	}
	public static String getCrawlDirName() {
		return crawlDirName;
	}

	public void setCrawlDirName(String crawlDirName) {
		SampleExporter.crawlDirName = crawlDirName;
	}

	public void setLanguage(String language) {
		SampleExporter.language = language;
	}

	public static String getLanguage() {
		return language;
	}

	public static String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		SampleExporter.topic = topic;
	}
	public void setKeepBoiler(boolean k) {
		SampleExporter.keepBoiler = k;
	}
	public static String getNegWordsFile() {
		return negWordsFile;
	}

	public void setNegWordsFile(String negWordsFile) {
		SampleExporter.negWordsFile = negWordsFile;
	}

	public void setOutputFile(String outputFile) {
		SampleExporter.outputFile  = outputFile;

	}
	public void setOutputDir(String outputDir) {
		SampleExporter.outputDir = outputDir;
	}
	public void setTextExport(boolean textexport){
		SampleExporter.textExport = textexport;
	}
}
