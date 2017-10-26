package gr.ilsp.fmc.bilingual.main;

import java.io.File;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class FmcBilingualOptions {
	private static final Logger LOGGER = Logger.getLogger(FmcBilingualOptions.class);
	private  Options options;
	private String _domain;
	private boolean _textexport;
	private String _outputDir;
	private String _agentName;
	private int _threads;
	private int _crawlDuration;
	private String _topic1;
	private String _topic2;
	private String _language1;
	private String _language2;
	private String _config;
	private boolean _keepBoiler;
	private int _length;
	private String APPNAME = "FMCBilingual";
	
	
	public FmcBilingualOptions() {
		createOptions();
	}
	@SuppressWarnings("static-access")
	private  Options createOptions() {
		options = new Options();		
		options.addOption( OptionBuilder.withLongOpt("domain")
				.withDescription( "domain to crawl (e.g. cnn.com) or path to file " +
						"with domains to crawl. Use for crawling ONLY inside specific domain(s)" )
				.hasArg()
				.create("d") );
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
		options.addOption( OptionBuilder.withLongOpt( "crawlduration" )
				.withDescription( "target crawl duration in minutes" )
				.hasArg()
				.create("c") );
		options.addOption( OptionBuilder.withLongOpt( "topic1" )
				.withDescription( "Topic definition for first language" )
				.hasArg()
				.create("tc1") );
		options.addOption( OptionBuilder.withLongOpt( "topic2" )
				.withDescription( "Topic definition for second language" )
				.hasArg()
				.create("tc2") );
		options.addOption( OptionBuilder.withLongOpt( "language1" )
				.withDescription( "First language." )
				.hasArg()
				.create("lang1") );
		options.addOption( OptionBuilder.withLongOpt( "language2" )
				.withDescription( "Second language." )
				.hasArg()
				.create("lang2") );
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
			
			if(line.hasOption( "d")) {
				_domain = line.getOptionValue("d");
				if (_domain.startsWith("http")) {
		            LOGGER.error("The target domain should be specified as just the host, without the http protocol: " + _domain);
		            //printUsageAndExit(parser);
		            help();
		        }
			} else help();
			
			if(line.hasOption( "te")) {
				_textexport  = true;
			}
			if(line.hasOption( "o")) {
				File of = new File(line.getOptionValue("o"));
				_outputDir = of.getAbsolutePath();									
			} else help();
			if(line.hasOption( "a")) {
				_agentName = line.getOptionValue("a");
			}			
			else help();
			if(line.hasOption( "t")) {
				_threads = Integer.parseInt(line.getOptionValue("t"));
			}			
			if(line.hasOption("c")) {
				_crawlDuration = Integer.parseInt(line.getOptionValue("c"));
			}			
			if(line.hasOption( "tc1")) {
				_topic1 = line.getOptionValue("tc1");
			}
			if(line.hasOption( "tc2")) {
				_topic2 = line.getOptionValue("tc2");
			}			
			if(line.hasOption( "lang1")) {
				_language1 = line.getOptionValue("lang1");
			}
			if(line.hasOption( "lang2")) {
				_language2 = line.getOptionValue("lang2");
			}						
			if(line.hasOption( "cfg")) {
				_config = line.getOptionValue("cfg");
			}		
			if(line.hasOption( "k")) {
				_keepBoiler  = true;
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
		printHelp( APPNAME  , options );
		System.exit(0);
	}
	public  void printHelp(String program, Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( program, options );
	}
	public Options getOptions() {
		return options;
	}
	public String get_domain() {
		return _domain;
	}
	public boolean is_textexport() {
		return _textexport;
	}
	public String get_outputDir() {
		return _outputDir;
	}
	public String get_agentName() {
		return _agentName;
	}
	public int get_threads() {
		return _threads;
	}
	public int get_crawlDuration() {
		return _crawlDuration;
	}
	public String get_topic1() {
		return _topic1;
	}
	public String get_topic2() {
		return _topic2;
	}
	public String get_language1() {
		return _language1;
	}
	public String get_language2() {
		return _language2;
	}
	public String get_config() {
		return _config;
	}
	public boolean is_keepBoiler() {
		return _keepBoiler;
	}
	public int get_length() {
		return _length;
	}
}
