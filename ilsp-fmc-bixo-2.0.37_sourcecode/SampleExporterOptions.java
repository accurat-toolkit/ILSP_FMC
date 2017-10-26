package gr.ilsp.fmc.exporter;



import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class SampleExporterOptions {
	
	private Options options;
	private String _inputdir;
	private String _language;
	private String _topic;
	private String _negwords = "";
	private String _outputdir = "";
	private String APPNAME = "SimpleCrawlHFS export";
	private int _length;
	private boolean _textexport=false;
	
	
	
	public SampleExporterOptions() {
		createOptions();
	}
	
	@SuppressWarnings("static-access")
	private  Options createOptions() {
		options = new Options();

		options.addOption( OptionBuilder.withLongOpt( "inputdir" )
				.withDescription( "Directory containing crawled data" )
				.hasArg()
				.create("i") );
		options.addOption( OptionBuilder.withLongOpt( "language" )
				.withDescription( "Target language of crawled data" )
				.hasArg()
				.create("l") );
		options.addOption( OptionBuilder.withLongOpt( "topic" )
				.withDescription( "Path to topic file" )	
				.hasArg()
				.create("t") );
		options.addOption( OptionBuilder.withLongOpt( "length" )
				.withDescription( "Minimum number of tokens per text block" )	
				.hasArg()
				.create("len") );
		options.addOption( OptionBuilder.withLongOpt( "negwords" )
				.withDescription( "Path to file containing negative words")
				.hasArg()
				.create("n") );
		options.addOption( OptionBuilder.withLongOpt( "outputdir" )
				.withDescription( "output directory" )
				.hasArg()
				.create("o") );	
		options.addOption( OptionBuilder.withLongOpt( "textexport" )
				.withDescription( "Export raw txt files" )				
				.create("te") );
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
			if(line.hasOption( "i")) {
				_inputdir = line.getOptionValue("i");
			}
			else help();
			
			if(line.hasOption( "l")) {
				_language = line.getOptionValue("l");;
			}
			if(line.hasOption( "t")) {
				_topic = line.getOptionValue("t");;
			} 
			if(line.hasOption( "len")) {
				_length = Integer.parseInt(line.getOptionValue("len"));
			} 
			if(line.hasOption( "n")) {
				_negwords = line.getOptionValue("n");;
			} 
			if(line.hasOption( "te")) {
				_textexport = true;
			} 
			if(line.hasOption( "o")) {
				_outputdir = line.getOptionValue("o");;
			} else help();						
			
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

	public String get_inputdir() {
		return _inputdir;
	}

	public String get_language() {
		return _language;
	}

	public String get_topic() {
		return _topic;
	}

	public String get_negwords() {
		return _negwords;
	}

	public String get_outputdir() {
		return _outputdir;
	}
	
	public int get_length() {
		return _length;
	}
	public boolean get_textexport(){
		return _textexport;
	}
 }
