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

public class SimpleCrawlHFSOptions {
	public static int NO_CRAWL_DURATION = 0;
	private  final String APPNAME = "ilsp-fmc-bixo.jar";
	private  Options options;
	private  String _domain=null;
	private  boolean _debug = false;
	private  String _loggingAppender = null;
	private  String _outputDir;
	private  String _outputFile;
	private  String _agentName;
	private  int _threads = 10;
	private  int _numLoops = 1;
	private  int _crawlDuration = 0;	
	private String _topic;
	private String _language;
	private String _urls;
	private boolean _keepBoiler = false;
	private boolean _force = false;
	private String _config;
	private int _length = 10;
	private String _operation;
	private boolean _textexport = false;
	private static final Logger LOGGER = Logger.getLogger(SimpleCrawlHFSOptions.class);
	
	
	
	public SimpleCrawlHFSOptions() {
		createOptions();
	}
	@SuppressWarnings("static-access")
	private  Options createOptions() {
		options = new Options();
		options.addOption( OptionBuilder.withLongOpt("operation")
				.withDescription( "operation to conduct: crawlandexport|crawl|export|config" )
				.hasArg()
				.create("op") );
		options.addOption( OptionBuilder.withLongOpt("domain")
				.withDescription( "domain to crawl (e.g. cnn.com) or path to file " +
						"with domains to crawl. Use for crawling ONLY inside specific domain(s)" )
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
		options.addOption( OptionBuilder.withLongOpt( "outputfile" )
				.withDescription( "output list file" )
				.hasArg()
				.create("of") );
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
		options.addOption( OptionBuilder.withLongOpt( "topic" )
				.withDescription( "Topic definition" )
				.hasArg()
				.create("tc") );
		options.addOption( OptionBuilder.withLongOpt( "language" )
				.withDescription( "Target language. If more than one, separate with ';', " +
						"i.e. en;el" )
				.hasArg()
				.create("lang") );
		options.addOption( OptionBuilder.withLongOpt( "config" )
				.withDescription( "XML file with configuration for the crawler." )
				.hasArg()
				.create("cfg") );
		options.addOption( OptionBuilder.withLongOpt( "keepboiler" )
				.withDescription( "Annotate boilerplate content in parsed text" )				
				.create("k") );
		options.addOption( OptionBuilder.withLongOpt( "force" )
				.withDescription( "Force to start new crawl. " +
						"Caution: This will remove any previous crawl data (if they exist)." )				
				.create("f") );	
		options.addOption( OptionBuilder.withLongOpt( "textexport" )
				.withDescription( "Export plain text files" )				
				.create("te") );
		options.addOption( OptionBuilder.withLongOpt( "help" )
				.withDescription( "Help" )
				.hasArg()
				.create("h") );
		options.addOption( OptionBuilder.withLongOpt( "length" )
				.withDescription( "Minimum number of tokens per text block" )	
				.hasArg()
				.create("len") );

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
			if(line.hasOption( "op")) {
				_operation = line.getOptionValue("op");
				// operation to conduct: crawlandexport|crawl|export|config" )
				if (!(_operation.equalsIgnoreCase("crawlandexport")
						|| _operation.equalsIgnoreCase("crawl")
						|| _operation.equalsIgnoreCase("export")
						||_operation.equalsIgnoreCase("config"))) {
					LOGGER.error("Allowed operations are " + "crawlandexport|crawl|export|config");
		            help();
		        }
				if (_operation.equals("config") && !line.hasOption("of")){
					LOGGER.error("Config operation requires the -of option to be set");
					help();
				}
			}else help();
			if(line.hasOption( "d")) {
				_domain = line.getOptionValue("d");
				if (_domain.startsWith("http")) {
		            LOGGER.error("The target domain should be specified as just the host, without the http protocol: " + _domain);
		            //printUsageAndExit(parser);
		            help();
		        }
			}
			if (line.hasOption("u")) {
				_urls = line.getOptionValue("u");
				File f = new File(_urls);
				if (f.exists()==false){
					LOGGER.error("The topic file does not exist.");
					help();
				}
			}
			
			if(line.hasOption( "dbg")) {
				_debug = true;
			}
			if(line.hasOption( "te")) {
				_textexport  = true;
			}
			if(line.hasOption( "l")) {
				_loggingAppender = line.getOptionValue("l");
			}
			if(line.hasOption( "o") && !_operation.equalsIgnoreCase("config")) {
				File of = new File(line.getOptionValue("o"));
				_outputDir = of.getAbsolutePath();
				if (line.hasOption("of")){
					of = new File(line.getOptionValue("of"));
					_outputFile = of.getAbsolutePath();
				} else
				_outputFile = _outputDir + System.getProperty("file.separator") + "outputlist.txt";					
			} /*else if (line.hasOption( "of")) {
				File of = new File(line.getOptionValue("of"));				 
				_outputFile = of.getAbsolutePath();
				File outputDir = new File("/var/lib/tomcat6/webapps/soaplab2-results/" + UUID.randomUUID().toString());
				_outputDir = outputDir.getAbsolutePath();
			}*/
			else if (_operation.equalsIgnoreCase("config") && line.hasOption("of")){
				File of = new File(line.getOptionValue("of"));
				_outputFile = of.getAbsolutePath();
			}
			else help();
			if(line.hasOption( "a")) {
				_agentName = line.getOptionValue("a");
			}			
			else if (_operation.equalsIgnoreCase("crawlandexport") ||
					_operation.equalsIgnoreCase("crawl"))
				help();
			if(line.hasOption( "t")) {
				_threads = Integer.parseInt(line.getOptionValue("t"));
			}			
			if(line.hasOption( "n")) {
				_numLoops = Integer.parseInt(line.getOptionValue("n"));
			}						
			if(line.hasOption("c")) {
				_crawlDuration = Integer.parseInt(line.getOptionValue("c"));
			}			
			if(line.hasOption( "tc")) {
				_topic = line.getOptionValue("tc");
			}			
			if(line.hasOption( "lang")) {
				_language = line.getOptionValue("lang");
			}						
			if(line.hasOption( "cfg")) {
				_config = line.getOptionValue("cfg");
			}		
			if(line.hasOption( "k")) {
				_keepBoiler  = true;
			}
			if(line.hasOption( "f")) {
				_force   = true;
			}
			if(line.hasOption( "len")) {
				_length = Integer.parseInt(line.getOptionValue("len"));
			} 

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
	public  String getOutputFile() {
		return _outputFile;
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
	public String getUrls() {
		return _urls;
	}
	public boolean keepBoiler() {
		return _keepBoiler;
	}
	public boolean Force() {
		return _force;
	}
	public String getConfig(){
		return _config;
	}
	public String getOperation(){
		return _operation;
	}

	public int getlength() {
		return _length;
	}
	
	public boolean getTextExport(){
		return _textexport;
	}

}
