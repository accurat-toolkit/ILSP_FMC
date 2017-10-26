package gr.ilsp.fmc.main;


import gr.ilsp.fmc.datums.CrawlDbDatum;
import gr.ilsp.fmc.mysql.MYSQLTapFactory;
import gr.ilsp.fmc.mysql.MYSQLTools;
import gr.ilsp.fmc.parser.DomainUrlFilter;
import gr.ilsp.fmc.utils.TopicTools;
import gr.ilsp.fmc.workflows.SimpleCrawlWorkflow;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;



import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.config.FetcherPolicy.FetcherMode;
import bixo.datum.UrlStatus;
import bixo.urls.BaseUrlFilter;
import bixo.urls.SimpleUrlNormalizer;
import bixo.utils.CrawlDirUtils;
import cascading.flow.Flow;
import cascading.flow.PlannerException;
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
public class SimpleCrawl {
	private static ArrayList<String[]> topic;
	private static String[] classes;
	private static final Logger LOGGER = Logger.getLogger(SimpleCrawl.class);
	public static XMLConfiguration config;
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

	private static void importOneDomain(String targetDomain, Tap urlSink, JobConf conf) throws IOException {

		TupleEntryCollector writer;
		try {
			writer = urlSink.openForWrite(conf);
			SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();
			CrawlDbDatum datum = new CrawlDbDatum(normalizer.normalize("http://" + targetDomain), 0, 0, UrlStatus.UNFETCHED, 0,0.0);

			writer.add(datum.getTuple());
			writer.close();
		} catch (IOException e) {
			throw e;
		}
	}
	private static void importUrlList(String urls, Tap urlSink, JobConf conf) throws IOException {
		TupleEntryCollector writer;        
		try {
			writer = urlSink.openForWrite(conf);
			SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();            
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(urls),"utf8"));
			String line = "";
			while ((line=rdr.readLine())!=null){
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
		SimpleCrawlOptions options = new SimpleCrawlOptions();
		options.parseOptions(args);
		boolean newDb = false;
		URL cc = SimpleCrawl.class.getClassLoader().getResource("crawler_config.xml");
		config = new XMLConfiguration();
		config.setURL(cc);
		try {
			config.load();
		} catch (ConfigurationException e1) {		
			e1.printStackTrace();
		}
		int min_uniq_terms = SimpleCrawl.config.getInt("classifier.min_unique_content_terms.value");
		int max_depth = SimpleCrawl.config.getInt("classifier.max_depth.value");
		// Before we get too far along, see if the domain looks valid.
		String domain = options.getDomain();
		String urls = null;
		if (domain==null) urls = options.getUrls();
		else {
			if (!domain.equals("localhost") && (domain.split("\\.").length < 2)) {
				LOGGER.error("The target domain should be a valid paid-level domain or subdomain of the same: " + domain);
				//printUsageAndExit(parser);
				options.help();
			}
		}		
		try {
			newDb = MYSQLTools.initializeDB(options.getDbName(), options.getDbHost());
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			LOGGER.error("Error while initializing database " + options.getDbName());
			System.exit(64);
		}
		
		//split the topic in triplets
		topic=TopicTools.analyzeTopic(options.getTopic(),options.getLanguage());
		//find the subclasses of the topic definition
		classes=TopicTools.findSubclasses(topic);
		double thres = TopicTools.calculateThreshold(topic,5);
		String outputDirName = options.getOutputDir();
		if (options.isDebug()) {
			System.setProperty("fmc.root.level", "DEBUG");            
		} else {
			System.setProperty("fmc.root.level", "INFO");            
		}

		if (options.getLoggingAppender() != null) {
			// Set console vs. DRFA vs. something else
			System.setProperty("fmc.appender", options.getLoggingAppender());
		}

		try {
			JobConf conf = new JobConf();
			conf.setJarByClass(SimpleCrawl.class);
			Path outputPath = new Path(outputDirName);
			FileSystem fs = outputPath.getFileSystem(conf);

			// See if the user is starting from scratch
			if (options.getDbHost() == null || newDb) {
				if (fs.exists(outputPath)) {
					System.out.println("Warning: Previous cycle output dirs exist in : " + outputDirName);
					System.out.println("Warning: Delete the output dir before running");
					fs.delete(outputPath, true);
				}
			}
			
			
			
			if (!fs.exists(outputPath)) {
				fs.mkdirs(outputPath);

				Path curLoopDir = CrawlDirUtils.makeLoopDir(fs, outputPath, 0);
				String curLoopDirName = curLoopDir.toUri().toString();
				setLoopLoggerFile(curLoopDirName, 0);
				if (domain!=null)
					importOneDomain(domain, MYSQLTapFactory.createUrlsSinkJDBCTap(options.getDbHost(), options.getDbName()), conf);
				else
					importUrlList(urls,MYSQLTapFactory.createUrlsSinkJDBCTap(options.getDbHost(), options.getDbName()), conf);
			}

			Path inputPath = CrawlDirUtils.findLatestLoopDir(fs, outputPath);

			if (inputPath == null) {
				System.err.println("No previous cycle output dirs exist in " + outputDirName);
				//printUsageAndExit(parser);
				options.help();
			}

			int startLoop = CrawlDirUtils.extractLoopNumber(inputPath);
			int endLoop = startLoop + options.getNumLoops();

			//UserAgent userAgent = new UserAgent(options.getAgentName(), CrawlConfig.EMAIL_ADDRESS, CrawlConfig.WEB_ADDRESS);
			UserAgent userAgent = new UserAgent(options.getAgentName(), config.getString("agent.email"), config.getString("agent.web_address"));

			FetcherPolicy defaultPolicy = new FetcherPolicy();
			//defaultPolicy.setCrawlDelay(CrawlConfig.DEFAULT_CRAWL_DELAY);
			defaultPolicy.setCrawlDelay(config.getLong("fetcher.crawl_delay.value"));
			//defaultPolicy.setMaxContentSize(CrawlConfig.MAX_CONTENT_SIZE);
			defaultPolicy.setMaxContentSize(config.getInt("fetcher.max_content_size.value"));
			defaultPolicy.setFetcherMode(FetcherMode.EFFICIENT);
			//defaultPolicy.setMaxRequestsPerConnection(500);
			defaultPolicy.setMaxRequestsPerConnection(config.getInt("fetcher.max_requests_per_run.value"));
			defaultPolicy.setMaxConnectionsPerHost(config.getInt("fetcher.max_connections_per_host.value"));
			// You can also provide a set of mime types you want to restrict what content type you 
	        // want to deal with - for now keep it simple.
			
			String[] mimes = config.getStringArray("fetcher.valid_mime_types.mime_type[@value]");			
	        Set<String> validMimeTypes = new HashSet<String>();
	        for (String s: mimes) validMimeTypes.add(s);
	        defaultPolicy.setValidMimeTypes(validMimeTypes);
	        //defaultPolicy.setMaxContentSize(500*1024);
			int crawlDurationInMinutes = options.getCrawlDuration();
			boolean hasEndTime = crawlDurationInMinutes != SimpleCrawlOptions.NO_CRAWL_DURATION;
			long targetEndTime = hasEndTime ? 
					System.currentTimeMillis() + (crawlDurationInMinutes * 60000L) : FetcherPolicy.NO_CRAWL_END_TIME;

					BaseUrlFilter urlFilter = new DomainUrlFilter(domain);

					// Now we're ready to start looping, since we've got our current settings
					for (int curLoop = startLoop + 1; curLoop <= endLoop; curLoop++) {

						// Adjust target end time, if appropriate.
						if (hasEndTime) {
							int remainingLoops = (endLoop - curLoop) + 1;
							long now = System.currentTimeMillis();
							long perLoopTime = (targetEndTime - now) / remainingLoops;
							defaultPolicy.setCrawlEndTime(now + perLoopTime);
						}

						Path curLoopDir = CrawlDirUtils.makeLoopDir(fs, outputPath, curLoop);
						String curLoopDirName = curLoopDir.toUri().toString();
						setLoopLoggerFile(curLoopDirName, curLoop);
						Flow flow = SimpleCrawlWorkflow.createFlow(inputPath, curLoopDir, userAgent, defaultPolicy, urlFilter, 
								options.getThreads(), options.isDebug(), options.getDbHost(), options.getDbName(), options.getLanguage(),
								classes, topic, thres, min_uniq_terms, max_depth);

						flow.complete();
						// flow.writeDOT("build/valid-flow.dot");

						// Input for the next round is our current output
						inputPath = curLoopDir;
					}
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
		MYSQLTapFactory.shutdown();
	}

}