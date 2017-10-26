package gr.ilsp.fmc.main;


import gr.ilsp.fmc.datums.CrawlDbDatum;
import gr.ilsp.fmc.exporter.SampleExporter;
import gr.ilsp.fmc.parser.DomainUrlFilter;
import gr.ilsp.fmc.utils.DirUtils;
import gr.ilsp.fmc.utils.TopicTools;
import gr.ilsp.fmc.workflows.SimpleCrawlHFSWorkflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.FileAppender;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;



import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.config.FetcherPolicy.FetcherMode;
import bixo.datum.UrlStatus;
import bixo.examples.CrawlConfig;
import bixo.urls.BaseUrlFilter;
import bixo.urls.SimpleUrlNormalizer;
import bixo.utils.CrawlDirUtils;
import cascading.flow.Flow;
import cascading.flow.PlannerException;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;

/**
 * JDBCCrawlTool is an example of using Bixo to write a simple crawl tool.
 * 
 * This tool uses an in-memory hsqldb to demonstrate how one could use a 
 * database to maintain the crawl db. 
 *  
 * 
 */
@SuppressWarnings("deprecation")
public class SimpleCrawlHFS {
	private static ArrayList<String[]> topic;
	private static String[] classes;
	private static final Logger LOGGER = Logger.getLogger(SimpleCrawlHFS.class);
	public static CompositeConfiguration config;
	private static int PAGES_STORED = 0;
	private static int PAGES_VISITED = 0;
	public static JobConf conf = null;


	private BaseConfiguration opt = new BaseConfiguration();

	public SimpleCrawlHFS(BaseConfiguration opt){
		this.opt = opt;
	}


	// Create log output file in loop directory.
	private static void setLoopLoggerFile(String outputDirName, int loopNumber) {
		Logger rootLogger = Logger.getRootLogger();

		String filename = String.format("%s/%d-JDBCCrawlTool.log", outputDirName, loopNumber);
		FileAppender appender = (FileAppender) rootLogger.getAppender("loop-logger");
		if (appender == null) {
			appender = new FileAppender();
			appender.setName("loop-logger");
			appender.setLayout(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}:%L - %m%n"));

			// We have to do this before calling addAppender, as otherwise Log4J
			// warns us.
			appender.setFile(filename);
			appender.activateOptions();
			rootLogger.addAppender(appender);
		} else {
			appender.setFile(filename);
			appender.activateOptions();
		}
	}

	private static void importOneDomain(String targetDomain, Path crawlDbPath, JobConf conf) throws IOException {
		try {
			Tap urlSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toUri().toString(), true);
			TupleEntryCollector writer = urlSink.openForWrite(conf);			
			SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();
			CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize("http://" + targetDomain), 0, 0, UrlStatus.UNFETCHED, 0,0.0);

			writer.add(datum.getTuple());
			writer.close();
			LOGGER.info("Added domain: " + datum.getUrl());
		} catch (IOException e) {
			throw e;
		}
	}
	private static void importDomains(Path dompath, Path crawlDbPath, JobConf conf) throws IOException {
		try {
			Tap urlSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toUri().toString(), true);
			TupleEntryCollector writer = urlSink.openForWrite(conf);			
			SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(dompath.toUri().getPath()),"utf8"));
			String line = "";
			while ((line=rdr.readLine())!=null){
				byte[] bts = line.getBytes("UTF-8");
				if (bts[0] == (byte) 0xEF && bts[1] == (byte) 0xBB && bts[2]==(byte) 0xBF) {
					byte[] bts2 = new byte[bts.length-3];
					for (int i = 3; i<bts.length;i++)
						bts2[i-3]=bts[i];
					line = new String(bts2);
				}
				if (line.equals("")) continue;
				CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize("http://" + line), 0, 0, 
						UrlStatus.UNFETCHED, 0,0.0);
				writer.add(datum.getTuple());
				LOGGER.info("Added domain: " + datum.getUrl());
			}
			rdr.close();
			//CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize("http://" + targetDomain), 0, 0, UrlStatus.UNFETCHED, 0,0.0);

			//writer.add(datum.getTuple());
			writer.close();
			
		} catch (IOException e) {
			throw e;
		}
	}
	private static void importUrlList(String urls, Path crawlDbPath, JobConf conf) throws IOException {		        
		try {
			Tap urlSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toUri().toString(), true);
			TupleEntryCollector writer = urlSink.openForWrite(conf);
			SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();            
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(urls),"utf8"));
			String line = "";
			while ((line=rdr.readLine())!=null){
				byte[] bts = line.getBytes("UTF-8");
				if (bts[0] == (byte) 0xEF && bts[1] == (byte) 0xBB && bts[2]==(byte) 0xBF) {
					byte[] bts2 = new byte[bts.length-3];
					for (int i = 3; i<bts.length;i++)
						bts2[i-3]=bts[i];
					line = new String(bts2);
				}
				if (line.equals("")) continue;
				CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize(line), 0, 0, 
						UrlStatus.UNFETCHED, 0,0.0);
				writer.add(datum.getTuple());
			}
			rdr.close();
			writer.close();			
		} catch (IOException e) {
			throw e;
		}
	}

	public static void main(String[] args) {

		SimpleCrawlHFSOptions options = new SimpleCrawlHFSOptions();
		options.parseOptions(args);
		BaseConfiguration opt = new BaseConfiguration();
		opt.addProperty("agent", options.getAgentName());
		opt.addProperty("config", options.getConfig());
		opt.addProperty("crawlduration",options.getCrawlDuration());
		opt.addProperty("domain", options.getDomain());
		opt.addProperty("language", options.getLanguage());
		opt.addProperty("length", options.getlength());
		opt.addProperty("loggingappender", options.getLoggingAppender());
		opt.addProperty("numloops", options.getNumLoops());
		opt.addProperty("outputdir", options.getOutputDir());
		opt.addProperty("outputfile", options.getOutputFile());
		opt.addProperty("threads", options.getThreads());
		opt.addProperty("topic", options.getTopic());
		opt.addProperty("urls", options.getUrls());
		opt.addProperty("operation", options.getOperation());
		opt.addProperty("isdebug", options.isDebug());
		opt.addProperty("force", options.Force());
		opt.addProperty("keepboiler", options.keepBoiler());
		opt.addProperty("textexport", options.getTextExport());
		SimpleCrawlHFS schfs = new SimpleCrawlHFS(opt);
		schfs.run();

		/*if (args.length==0){LOGGER.info("Usage: SimpleCrawlHFS [crawl|export|config]");
		System.exit(-1);
		}
		operation = args[0].toLowerCase();
		if (operation.equals("export")){
			SampleExporter.main(args);
		} else if (operation.equals("config")){
			if (args.length>1){
				String out = args[1];			
				URL default_config = SimpleCrawlHFS.class.getClassLoader().getResource("crawler_config.xml");
				XMLConfiguration xml;
				try {
					xml = new XMLConfiguration(default_config);
					//xml.load();
					xml.save(out);
					LOGGER.info("Saved default config file at " + out);
				} catch (ConfigurationException e) {
					// Shouldn't happen
					LOGGER.error("Couldn't save file " + out);
				}			
			} else LOGGER.error("Usage: SimpleCrawlHFS config <file to save config xml>");
		} else if (operation.equals("crawl") || operation.equals("crawlandexport")) {
			try {
				crawl(args);
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
			}
		} else {
			LOGGER.error("Invalid operation.");
			System.exit(-1);
		}*/
	}

	public void run() {
		String operation = opt.getString("operation");
		if (operation.equals("export")){
			//SampleExporter.main(args);
			loadLanguageProfile();
			SampleExporter se = new SampleExporter();
			se.setMIN_TOKENS_PER_PARAGRAPH(opt.getInt("length"));
			se.setLanguage(opt.getString("language"));
			se.setCrawlDirName (opt.getString("outputdir"));
			se.setOutputFile (opt.getString("outputfile"));					
			se.setTopic(opt.getString("topic"));
			se.setTextExport(opt.getBoolean("textexport"));
			se.setKeepBoiler(opt.getBoolean("keepboiler"));
			se.export(false);
		} else if (operation.equals("config")){
			String out = opt.getString("outputfile");			
			URL defaultConfig = SimpleCrawlHFS.class.getClassLoader().getResource("crawler_config.xml");
			XMLConfiguration xmlConfig;
			try {
				xmlConfig = new XMLConfiguration(defaultConfig);
				xmlConfig.save(out);
				LOGGER.info("Saved default config file at " + out);
			} catch (ConfigurationException e) {
				// Shouldn't happen
				LOGGER.error("Couldn't save file " + out);
			}			
		} else if (operation.equals("crawl")) {
			crawl();
		} else if (operation.equals("crawlandexport")) {
			crawl();
			SampleExporter se = new SampleExporter();
			se.setMIN_TOKENS_PER_PARAGRAPH(opt.getInt("length"));
			se.setLanguage(opt.getString("language"));
			se.setCrawlDirName (opt.getString("outputdir"));
			se.setOutputFile (opt.getString("outputfile"));					
			se.setTopic(opt.getString("topic"));	
			se.setTextExport(opt.getBoolean("textexport"));
			se.setKeepBoiler(opt.getBoolean("keepboiler"));
			se.export(false);		
		} else {
			LOGGER.error("Invalid operation.");
			System.exit(-1);
		}
	}

	private void crawl() {		

		//Loading the default configuration file and checking if user supplied a custom one.
		URL default_config = SimpleCrawlHFS.class.getClassLoader().getResource("crawler_config.xml");			
		config = new CompositeConfiguration();			
		if (opt.getString("config")!=null){
			String custom_config = opt.getString("config");
			try {
				XMLConfiguration xml_custom_config = new XMLConfiguration(custom_config);
				xml_custom_config.setValidating(true);
				config.addConfiguration(xml_custom_config);
			} catch (ConfigurationException e) {
				LOGGER.error("Invalid configuration file: " + custom_config);
			}
		}
		try {			
			config.addConfiguration(new XMLConfiguration(default_config));				
		} catch (ConfigurationException e1) {
			// Shouldn't happen
			LOGGER.error("Problem with default configuration file.");
		}

		conf = new JobConf();
		conf.setJarByClass(SimpleCrawlHFS.class);		
		conf.set("mapred.system.dir",conf.get("hadoop.tmp.dir") + "/mapred/system-"+ System.currentTimeMillis());
		conf.set("mapred.local.dir",conf.get("hadoop.tmp.dir") + "/mapred/local-"+ System.currentTimeMillis());
		//conf.set("hadoop.tmp.dir", "/tmp/hadoop-temp");
		FileSystem fs;
		//if domain is supplied, it is checked for errors
		String domain = opt.getString("domain");
		String urls = null;
		boolean isDomainFile = true;
		Path dompath = null;
		if (domain==null) {urls = opt.getString("urls");isDomainFile = false;}
		else {
			dompath = new Path(domain);
			try {
				fs = dompath.getFileSystem(conf);
				if (!fs.exists(dompath)) {
					isDomainFile = false;
					if (!domain.equals("localhost") && (domain.split("\\.").length < 2)) {
						LOGGER.error("The target domain should be a valid paid-level domain or subdomain of the same: " + domain);
						//printUsageAndExit(parser);
						System.exit(64);
					}
				} //else urls = opt.getString("urls");
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
				System.exit(64);
			}

		}		

		loadLanguageProfile();

		int min_uniq_terms = SimpleCrawlHFS.config.getInt("classifier.min_unique_content_terms.value");
		int max_depth = SimpleCrawlHFS.config.getInt("classifier.max_depth.value");
		//Analyze the topic and create the classes and topic variables. Term threshold
		//is calculate based on the median weight of the terms and the minimum number
		//of terms each text must have as defined on the config file.
		double thres = 0;
		if (opt.getString("topic")!=null){
			topic=TopicTools.analyzeTopic(opt.getString("topic"),opt.getString("language"));
			LOGGER.info("Topic analyzed, " + topic.size() + " terms found.");
			//find the subclasses of the topic definition
			classes=TopicTools.findSubclasses(topic);
			LOGGER.info(classes.length + " classes found.");
			thres = TopicTools.calculateThreshold(topic,
					SimpleCrawlHFS.config.getInt("classifier.min_content_terms.value"));
			LOGGER.info("Classifier threshold calculated: "+ thres);
		} else LOGGER.info("Running with no topic.");


		String outputDirName = opt.getString("outputdir");
		if (opt.getBoolean("isdebug")) {
			System.setProperty("fmc.root.level", "DEBUG");            
		} else {
			System.setProperty("fmc.root.level", "INFO");            
		}

		if (opt.getString("loggingappender") != null) {
			// Set console vs. DRFA vs. something else
			System.setProperty("fmc.appender", opt.getString("loggingappender"));
		}

		try {
			//JobConf conf = new JobConf();
			//conf.setJarByClass(SimpleCrawlHFS.class);
			Path outputPath = new Path(outputDirName);				
			fs = outputPath.getFileSystem(conf);
			outputPath = outputPath.makeQualified(fs);
			//If force is used, the outputPath will be deleted if it already exists
			if (opt.getBoolean("force") && fs.exists(outputPath)){
				LOGGER.warn("Removing previous crawl data in " + outputPath);
				fs.delete(outputPath);
			}
			
			//If the outputPath does not exist, it is created. Seed URL list (or domain) is imported
			//into the hfs.
			if (!fs.exists(outputPath)) {				
				LOGGER.info("Creating path: " +outputPath);
				fs.mkdirs(outputPath);
			}
			if (fs.listStatus(outputPath).length==0){
				Path curLoopDir = CrawlDirUtils.makeLoopDir(fs, outputPath, 0);
				String curLoopDirName = curLoopDir.toUri().toString();
				if (curLoopDirName.startsWith("file:/")) curLoopDirName = curLoopDirName.substring(5);
				setLoopLoggerFile(curLoopDirName, 0);
				Path crawlDbPath = new Path(curLoopDir, CrawlConfig.CRAWLDB_SUBDIR_NAME);
				if (domain!=null && !isDomainFile){						
					importOneDomain(domain,crawlDbPath , conf);
				} else if (isDomainFile){
					importDomains(dompath,crawlDbPath,conf);
				}
				else
					importUrlList(urls,crawlDbPath, conf);
			} 
			//The last run folder is detected (in case we are resuming a previous crawl)
			Path inputPath = CrawlDirUtils.findLatestLoopDir(fs, outputPath);
			if (inputPath == null) {
				System.err.println("No previous cycle output dirs exist in " + outputDirName);
				//printUsageAndExit(parser);
				System.exit(64);
			}
			//CrawlDbPath is the path where the crawl database will be stored for the current run
			Path crawlDbPath = new Path(inputPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);

			//Start and end loop numbers are calculated (if the crawl is running on a fixed
			//number of loops)
			int startLoop = CrawlDirUtils.extractLoopNumber(inputPath);
			int endLoop = startLoop + opt.getInt("numloops");

			UserAgent userAgent = new UserAgent(opt.getString("agent"), config.getString("agent.email"), config.getString("agent.web_address"));

			//Fetch policy configuration
			FetcherPolicy defaultPolicy = new FetcherPolicy();
			defaultPolicy.setCrawlDelay(config.getLong("fetcher.crawl_delay.value"));
			defaultPolicy.setFetcherMode(FetcherMode.EFFICIENT);
			defaultPolicy.setRequestTimeout(config.getLong("fetcher.request_timeout.value"));
			defaultPolicy.setMaxRequestsPerConnection(config.getInt("fetcher.max_requests_per_run.value"));
			defaultPolicy.setMaxConnectionsPerHost(config.getInt("fetcher.max_connections_per_host.value"));
			defaultPolicy.setMinResponseRate(config.getInt("fetcher.min_response_rate.value"));
			defaultPolicy.setMaxRedirects(config.getInt("fetcher.max_redirects.value"));
			defaultPolicy.setMaxContentSize(config.getInt("fetcher.max_content_size.value"));
			//Loading of acceptable MIME types from the config file
			String[] mimes = config.getStringArray("fetcher.valid_mime_types.mime_type[@value]");			
			Set<String> validMimeTypes = new HashSet<String>();
			for (String s: mimes) validMimeTypes.add(s);
			defaultPolicy.setValidMimeTypes(validMimeTypes);
			int crawlDurationInMinutes = opt.getInt("crawlduration");
			//hasEndTime is the time the crawl must end (if crawl is running on specified duration)
			boolean hasEndTime = crawlDurationInMinutes != SimpleCrawlHFSOptions.NO_CRAWL_DURATION;
			long targetEndTime = hasEndTime ? System.currentTimeMillis() + 	(crawlDurationInMinutes * 60000L) : FetcherPolicy.NO_CRAWL_END_TIME;
			//Setting up the URL filter. If domain is supplied, the filter will disregard all
			//URLs that do not belong in the specified web domain.
			BaseUrlFilter urlFilter = null;
			if (isDomainFile)
				urlFilter = new DomainUrlFilter(dompath);
			else
				urlFilter = new DomainUrlFilter(domain);

			// Main loop. This will run as many times as specified by the numloop option
			//or until the specified duration is reached
			long startTime = System.currentTimeMillis();
			for (int curLoop = startLoop + 1; curLoop <= endLoop; curLoop++) {
				// Checking if duration is expired. If so, crawling is terminated.
				if (hasEndTime) {
					long now = System.currentTimeMillis();
					if (targetEndTime-now<=0){
						LOGGER.info("Time expired, ending crawl.");
						long duration = System.currentTimeMillis()-startTime;
						LOGGER.info("Made " + (curLoop-startLoop-1) + " runs in " + 
								(System.currentTimeMillis()-startTime) + " milliseconds.");
						float avg = (float)duration/(curLoop-startLoop-1);
						LOGGER.info("Total pages stored/visited: " + PAGES_STORED + "/" + PAGES_VISITED);
						LOGGER.info("Average run time: " + avg + " milliseconds.");						
						break;
					}
					//If duration is not reached, endLoop is increased to run the next loop
					endLoop = curLoop + 1;
				}
				//The workflow is created and launched. Each flow is created using the directory of the
				//current loop, the crawl db directory, the policy for the Fetcher, the URL filter, the
				//topic and classes arrays, the term threshold and the crawl options
				Path curLoopDir = CrawlDirUtils.makeLoopDir(fs, outputPath, curLoop);
				String curLoopDirName = curLoopDir.toUri().toString();
				if (curLoopDirName.startsWith("file:/")) curLoopDirName = curLoopDirName.substring(5);
				setLoopLoggerFile(curLoopDirName, curLoop);	
				//Flow flow = SimpleCrawlHFSWorkflow.createFlow(curLoopDir, crawlDbPath, userAgent, defaultPolicy, urlFilter, 
				//		classes, topic, thres,min_uniq_terms,max_depth,opt);							
				//flow.complete();
				
				SimpleCrawlHFSWorkflow schwf = new SimpleCrawlHFSWorkflow();				
				Flow flow = schwf.createFlow(curLoopDir, crawlDbPath, userAgent, defaultPolicy, urlFilter, 
						classes, topic, thres,min_uniq_terms,max_depth, opt.getInt("threads"), opt.getBoolean("isdebug"),
						opt.getBoolean("keepboiler"), opt.getString("language"),true);
				
				flow.complete();

			
				//Reseting counters of parent class. We do it here so that SplitFetchedUnfetchedCrawlDatums
				//when run again will not return the 256(or whatever) that were selected in the first run
				SimpleCrawlHFSWorkflow.resetCounters();
				if (curLoop>3) {
					DirUtils.clearPreviousLoopDir(fs,outputPath,curLoop);
				}

				LOGGER.info("Total pages stored/visited: " + PAGES_STORED + "/" + PAGES_VISITED);
				// flow.writeDOT("build/valid-flow.dot");

				// Input for the next round is our current output
				crawlDbPath = new Path(curLoopDir, CrawlConfig.CRAWLDB_SUBDIR_NAME);
				//inputPath = curLoopDir;
			}
			// Finished crawling. Now export if needed.
			/*String operation = opt.getString("operation");
			if (operation.equals("crawlandexport")) {
				SampleExporter se = new SampleExporter();
				se.setMIN_TOKENS_PER_PARAGRAPH(opt.getInt("length"));
				se.setLanguage(opt.getString("language"));
				se.setCrawlDirName (outputDirName);
				se.setOutputFile (opt.getString("outputfile"));					
				se.setTopic(opt.getString("topic"));	
				se.setTextExport(opt.getBoolean("textexport"));
				se.setKeepBoiler(opt.getBoolean("keepboiler"));
				se.export(false);
			}*/

		} catch (PlannerException e) {
			e.writeDOT("build/failed-flow.dot");
			System.err.println("PlannerException: " + e.getMessage());
			e.printStackTrace(System.err);
			System.exit(-1);
		} catch (Throwable t) {
			System.err.println("Exception running tool: " + t.getMessage());
			t.printStackTrace(System.err);
			System.exit(-1);
		}
	} 

	private void loadLanguageProfile(){
		String path = "profs/";
		InputStream is = null;
		URL urldir = SimpleCrawlHFS.class.getResource("/profs");
		if (urldir.getProtocol()=="jar"){			
			String jarPath = urldir.getPath().substring(5, urldir.getPath().indexOf("!")).replace("%20", " "); //strip out only the JAR file			
			JarFile jar;
			try {
				jar = new JarFile(jarPath);
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
				DetectorFactory.loadProfile(profs);
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
			} catch (LangDetectException e) {
				LOGGER.error(e.getMessage());
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

	public static void incrementPagesStored() {
		PAGES_STORED++;
	}
	public static void incrementPagesVisited() {
		PAGES_VISITED++;
	}


	public static ArrayList<String[]> getTopic() {
		return topic;
	}
}