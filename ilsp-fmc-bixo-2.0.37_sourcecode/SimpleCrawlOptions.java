package gr.ilsp.fmc.main;


import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class SimpleCrawlOptions {
	public static int NO_CRAWL_DURATION = 0;
	private  final String APPNAME = "SimpleCrawl";
	private  Options options;
	private  String _domain=null;
	private  boolean _debug = false;
	private  String _loggingAppender = null;
	private  String _outputDir;
	private  String _agentName;
	private  int _threads = 10;
	private  int _numLoops = 1;
	private  int _crawlDuration = 0;
	private  String _dbHost;
	private String _topic;
	private String _language;
	private String _urls;
	private String _dbName;
	private static final Logger LOGGER = Logger.getLogger(SimpleCrawlOptions.class);
	
	
	
	public SimpleCrawlOptions() {
		createOptions();
	}
	@SuppressWarnings("static-access")
	private  Options createOptions() {
		options = new Options();

		options.addOption( OptionBuilder.withLongOpt( "domain" )
				.withDescription( "domain to crawl (e.g. cnn.com). Use for crawling ONLY inside one domain" )
				.hasArg()
				.create("d") );
		options.addOption( OptionBuilder.withLongOpt( "urls" )
				.withDescription( "file with list of urls to crawl" )
				.hasArg()
				.create("u") );
		options.addOption( OptionBuilder.withLongOpt( "debug" )
				.withDescription( "debug logging" )				
				.create("dbg") );
		options.addOption( OptionBuilder.withLongOpt( "loggingAppender" )
				.withDescription( "set logging appender (console, DRFA)")
				.hasArg()
				.create("l") );
		options.addOption( OptionBuilder.withLongOpt( "outputdir" )
				.withDescription( "output directory" )
				.hasArg()
				.create("o") );
		options.addOption( OptionBuilder.withLongOpt( "agentname" )
				.withDescription( "user agent name" )
				.hasArg()
				.create("a") );
		options.addOption( OptionBuilder.withLongOpt( "threads" )
				.withDescription( "maximum number of fetcher threads to use" )
				.hasArg()
				.create("t") );		
		options.addOption( OptionBuilder.withLongOpt( "numloops" )
				.withDescription( "number of fetch/update loops" )
				.hasArg()
				.create("n") );		
		options.addOption( OptionBuilder.withLongOpt( "crawlduration" )
				.withDescription( "target crawl duration in minutes" )
				.hasArg()
				.create("c") );
		options.addOption( OptionBuilder.withLongOpt( "dbhost" )
				.withDescription( "Database host" )
				.hasArg()
				.create("db") );
		options.addOption( OptionBuilder.withLongOpt( "dbname" )
				.withDescription( "Database name" )
				.hasArg()
				.create("dn") );
		options.addOption( OptionBuilder.withLongOpt( "topic" )
				.withDescription( "Topic definition" )
				.hasArg()
				.create("tc") );
		options.addOption( OptionBuilder.withLongOpt( "language" )
				.withDescription( "Target language" )
				.hasArg()
				.create("lang") );
		options.addOption( OptionBuilder.withLongOpt( "help" )
				.withDescription( "Help" )
				.hasArg()
				.create("h") );
		return options;
	}
    
	public  void parseOptions( String[] args) {
		// create the command line parser
		CommandLineParser clParser = new GnuParser();
		try {
			CommandLine line = clParser.parse( options, args );

			if(line.hasOption( "h")) {
				help();
			}
			if(line.hasOption( "d")) {
				_domain = line.getOptionValue("d");
				if (_domain.startsWith("http")) {
		            LOGGER.error("The target domain should be specified as just the host, without the http protocol: " + _domain);
		            //printUsageAndExit(parser);
		            help();
		        }
			}
			else if (line.hasOption("u")) {
				_urls = line.getOptionValue("u");
				File f = new File(_urls);
				if (f.exists()==false){
					LOGGER.error("The topic file does not exist.");
					help();
				}
			}
			else help();
			if(line.hasOption( "dbg")) {
				_debug = true;
			}
			if(line.hasOption( "l")) {
				_loggingAppender = line.getOptionValue("l");
			}
			if(line.hasOption( "o")) {
				_outputDir = line.getOptionValue("o");
			}			
			else help();
			if(line.hasOption( "a")) {
				_agentName = line.getOptionValue("a");
			}			
			else help();
			if(line.hasOption( "t")) {
				_threads = Integer.parseInt(line.getOptionValue("t"));
			}			
			if(line.hasOption( "n")) {
				_numLoops = Integer.parseInt(line.getOptionValue("n"));
			}			
			if(line.hasOption( "c")) {
				_crawlDuration = Integer.parseInt(line.getOptionValue("c"));
			}			
			if(line.hasOption( "db")) {
				_dbHost = line.getOptionValue("db");
			}			
			else help();
			if(line.hasOption( "dn")) {
				_dbName = line.getOptionValue("dn");
			}			
			else help();
			if(line.hasOption( "tc")) {
				_topic = line.getOptionValue("tc");
			}			
			if(line.hasOption( "lang")) {
				_language = line.getOptionValue("lang");
			}			
			else help();
			
		} catch( ParseException exp ) {
			// oops, something went wrong
			System.err.println( "Parsing options failed.  Reason: " + exp.getMessage() );			
			System.exit(64);
		}
	}
	public  void help(){
		printHelp( APPNAME , options );
		System.exit(0);
	}
	public  void printHelp(String program, Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( program, options );
	}
	public String getLanguage() { return _language;}
	public String getTopic() { return _topic;}
	public  String getDomain() {
		return _domain;
	}
	public  boolean isDebug() {
		return _debug;
	}
	public  String getLoggingAppender() {
		return _loggingAppender;
	}
	public  String getOutputDir() {
		return _outputDir;
	}
	public  String getAgentName() {
		return _agentName;
	}
	public  int getThreads() {
		return _threads;
	}
	public  int getNumLoops() {
		return _numLoops;
	}
	public  int getCrawlDuration() {
		return _crawlDuration;
	}
	public  String getDbHost() {
		return _dbHost;
	}
	public String getDbName(){
		return _dbName;
	}
	public String getUrls() {
		return _urls;
	}
}
