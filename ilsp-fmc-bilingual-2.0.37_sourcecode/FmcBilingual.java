package gr.ilsp.fmc.bilingual.main;


import gr.ilsp.fmc.main.SimpleCrawlHFS;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.log4j.Logger;

public class FmcBilingual {
	private static final Logger LOGGER = Logger.getLogger(FmcBilingual.class);
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		FmcBilingualOptions options = new FmcBilingualOptions();
		options.parseOptions(args);
		BaseConfiguration opt = new BaseConfiguration();
		opt.addProperty("agent", options.get_agentName());
		opt.addProperty("config", options.get_config());
		opt.addProperty("crawlduration",options.get_crawlDuration());
		opt.addProperty("domain", options.get_domain());
		opt.addProperty("language", options.get_language1());
		opt.addProperty("length", options.get_length());
		opt.addProperty("outputdir", options.get_outputDir() + options.get_language1());
		opt.addProperty("threads", options.get_threads());
		opt.addProperty("isdebug", false);
		opt.addProperty("force", false);
		opt.addProperty("numloops", 1);
		opt.addProperty("outputfile", options.get_outputDir() + options.get_language1() + System.getProperty("file.separator") + "outputlist.txt");
		opt.addProperty("topic", options.get_topic1());
		opt.addProperty("operation", "crawlandexport");
		opt.addProperty("keepboiler", options.is_keepBoiler());
		opt.addProperty("textexport", options.is_textexport());
		LOGGER.info("Crawling for languages: " + options.get_language1()
				+ " " + options.get_language2());
		LOGGER.info("Starting crawl job for language " + options.get_language1());
		SimpleCrawlHFS schfs = new SimpleCrawlHFS(opt);
		schfs.run();		
		LOGGER.info("Starting crawl job for language " + options.get_language2());
		opt = new BaseConfiguration();
		opt.addProperty("agent", options.get_agentName());
		opt.addProperty("config", options.get_config());
		opt.addProperty("crawlduration",options.get_crawlDuration());
		opt.addProperty("domain", options.get_domain());
		opt.addProperty("language", options.get_language2());
		opt.addProperty("length", options.get_length());
		opt.addProperty("outputdir", options.get_outputDir() + options.get_language2());
		opt.addProperty("threads", options.get_threads());
		opt.addProperty("isdebug", false);
		opt.addProperty("force", false);
		opt.addProperty("numloops", 1);
		opt.addProperty("outputfile", options.get_outputDir() + options.get_language2() + System.getProperty("file.separator") + "outputlist.txt");
		opt.addProperty("topic", options.get_topic2());
		opt.addProperty("operation", "crawlandexport");
		opt.addProperty("keepboiler", options.is_keepBoiler());
		opt.addProperty("textexport", options.is_textexport());
		schfs = new SimpleCrawlHFS(opt);
		schfs.run();
	}

}
