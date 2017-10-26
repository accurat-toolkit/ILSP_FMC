package gr.ilsp.fmc.prealigner.main;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class PrealignerOptions {
	private Logger LOGGER;
	private static final String APPNAME = "Prealigner.jar";
	private Options options;
	private String _input1;
	private String _input2;
	private String _topic1;
	private String _topic2;
	private String _output;
	private String _logfile;
	private int _threshold;
	public PrealignerOptions(Logger LOGGER){
		this.LOGGER = LOGGER;
		createOptions();
	}
	
	@SuppressWarnings("static-access")
	private Options createOptions(){
		options = new Options();
		options.addOption(OptionBuilder.withLongOpt("inputdir1")
				.withDescription("Input directory for first language")				
				.hasArg()
				.create("i1"));
		options.addOption(OptionBuilder.withLongOpt("inputdir2")
				.withDescription("Input directory for second language")
				.hasArg()
				.create("i2"));
		options.addOption(OptionBuilder.withLongOpt("output")
				.withDescription("Full path of file to output results")
				.hasArg()
				.create("o"));
		options.addOption(OptionBuilder.withLongOpt("topic1")
				.withDescription("Topic for first language")
				.hasArg()
				.create("t1"));
		options.addOption(OptionBuilder.withLongOpt("topic2")
				.withDescription("Topic for second language")
				.hasArg()
				.create("t2"));
		options.addOption(OptionBuilder.withLongOpt("logfile")
				.withDescription("A file to store logs")
				.hasArg()
				.create("l"));	
		options.addOption(OptionBuilder.withLongOpt("threshold")
				.withDescription("Threshold for storing pairs. Value must be integer.")
				.hasArg()
				.create("t"));
		options.addOption(OptionBuilder.withLongOpt("help")
				.withDescription("Usage")
				.create("h"));
		return options;
	}



	public void parseOptions(String[] args) {
		CommandLineParser clParser = new GnuParser();
		try {
			CommandLine line = clParser.parse(options, args);
			if (line.hasOption("h"))
				help();
			if (line.hasOption("i1")){
				_input1 = line.getOptionValue("i1");
			} else help();
			if (line.hasOption("i2")){
				_input2 = line.getOptionValue("i2");
			} else help();
			if (line.hasOption("t1")){
				_topic1 = line.getOptionValue("t1");
			} else help(); 
			if (line.hasOption("t2")){
				_topic2 = line.getOptionValue("t2");
			} else help();
			if (line.hasOption("o")){
				_output = line.getOptionValue("o");
			} else help();
			if (line.hasOption("t")){				
				_threshold = Integer.parseInt(line.getOptionValue("t"));				
			} else help();
			if (line.hasOption("l")){
				_logfile = line.getOptionValue("l");
			} else help();			
		} catch (ParseException exp) {
			LOGGER.error("Parsing options failed. Reason: " + exp.getMessage());			
			System.exit(-1);
		}		
	}
	private void help(){
		printHelp(APPNAME , options);
		System.exit(0);
	}
	
	private void printHelp(String program, Options options){
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(program, options);
	}

	public String get_input1() {
		return _input1;
	}

	public String get_input2() {
		return _input2;
	}

	public String get_topic1() {
		return _topic1;
	}

	public String get_topic2() {
		return _topic2;
	}

	public String get_output() {
		return _output;
	}
	
	public int get_threshold() {
		return _threshold;
	}
	
	public String get_logfile() {
		return _logfile;
	}
}
