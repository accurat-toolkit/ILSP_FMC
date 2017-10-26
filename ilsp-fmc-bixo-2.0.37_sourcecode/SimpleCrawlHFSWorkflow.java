package gr.ilsp.fmc.workflows;

import gr.ilsp.fmc.classifier.Classifier;
import gr.ilsp.fmc.datums.ClassifierDatum;
import gr.ilsp.fmc.datums.CrawlDbDatum;
import gr.ilsp.fmc.datums.ExtendedParsedDatum;
import gr.ilsp.fmc.datums.ExtendedUrlDatum;
import gr.ilsp.fmc.main.SimpleCrawlHFS;
import gr.ilsp.fmc.operations.CreateCrawlDbDatumFromUrlFunction;
import gr.ilsp.fmc.operations.CreateUrlDatumFromCrawlDbFunction;
import gr.ilsp.fmc.operations.CreateUrlDatumFromStatusFunction;
import gr.ilsp.fmc.operations.ExtendedNormalizeUrlFunction;
import gr.ilsp.fmc.operations.MakeDistinctCrawlDbFunction;
import gr.ilsp.fmc.operations.SelectUrlOnlyFunction;
import gr.ilsp.fmc.parser.ExtendedUrlFilter;
import gr.ilsp.fmc.parser.RobotRulesParser;
import gr.ilsp.fmc.parser.SimpleNoLinksParser;
import gr.ilsp.fmc.pipes.ClassifierPipe;
import gr.ilsp.fmc.pipes.ExtendedParsePipe;
import gr.ilsp.fmc.utils.CrawlConfig;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import org.apache.log4j.Logger;

import bixo.config.DefaultFetchJobPolicy;
import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.BaseFetcher;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.operations.BaseScoreGenerator;
import bixo.operations.FixedScoreGenerator;
import bixo.pipes.FetchPipe;
import bixo.robots.RobotUtils;
import bixo.urls.BaseUrlFilter;
import bixo.urls.SimpleUrlNormalizer;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.LeftJoin;
import cascading.pipe.cogroup.RightJoin;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.bixolabs.cascading.BaseSplitter;
import com.bixolabs.cascading.HadoopUtils;
import com.bixolabs.cascading.SplitterAssembly;

@SuppressWarnings("deprecation")
public class SimpleCrawlHFSWorkflow {
	private static final Logger LOGGER = Logger.getLogger(SimpleCrawlHFSWorkflow.class);
	//BUFFER_SIZE represents maximum number of URLs per run
	private static final int BUFFER_SIZE = SimpleCrawlHFS.config.getInt("fetcher.fetch_buffer_size.value");
	//_numSelected is used as a counter for URLs selected for this run
	private static long _numSelected = 0;
	//hostsMap and newHostsMap represent the pairs of hosts-occurences globally and per-run respectively
	private static HashMap<Integer,Integer> hostsMap = new HashMap<Integer,Integer>();
	private static HashMap<Integer,Integer> hostsIpMap = new HashMap<Integer,Integer>();
	//statusSet contains all different status levels that are considered equal when selecting URLs to fetch
	private static final HashSet<String> statusSet = new HashSet<String>(Arrays.asList(new String[] {
			"SKIPPED_DEFERRED",
			"SKIPPED_BY_SCORER",
			"SKIPPED_BY_SCORE",
			"SKIPPED_TIME_LIMIT",
			"SKIPPED_INTERRUPTED",
			"SKIPPED_INEFFICIENT",
			"SKIPPED_PER_SERVER_LIMIT",
			"UNFETCHED"
	}));

	//This method basically selects the BUFFER_SIZE number of URLs to be visited in this run
	private static class SplitFetchedUnfetchedCrawlDatums extends BaseSplitter {		
		private static final long serialVersionUID = -5255131937144107833L;
		private static final int urlsPerServer = SimpleCrawlHFS.config.getInt("fetcher.max_fetched_per_host.value"); 
		private static final int urlsPerServerPerRun = SimpleCrawlHFS.config.getInt("fetcher.max_requests_per_host_per_run.value"); 
		
		@Override
		public String getLHSName() {
			return "fetched unfetched UrlDatums";
		}                
						
		@Override
		public boolean isLHS(TupleEntry tupleEntry) {			
			if (_numSelected>=BUFFER_SIZE) return false;
			CrawlDbDatum datum = new CrawlDbDatum(tupleEntry);
			UrlStatus status = datum.getLastStatus(); 
			URL url = null;
			Integer count = null, ipCount = null;
			String host = null; 
			int hostIpHash = 0,hostHash = 0;
			InetAddress ipaddress = null;
			
			try {
				url = new URL(datum.getUrl());
				host = url.getHost();
				hostHash = url.getHost().hashCode();				
				count = hostsMap.get(hostHash);				
			} catch (MalformedURLException e1) {
				LOGGER.error(e1.getMessage());
			} 
			
			
			if (statusSet.contains(status.name())) {
				if (count==null)
					hostsMap.put(hostHash, 1);
				else {
					count++;
					hostsMap.put(hostHash, count);
					if (count>urlsPerServer)
						return false;
				}
				try {
					ipaddress = InetAddress.getByName(host);
					hostIpHash = ipaddress.getHostAddress().hashCode();				
				} catch (UnknownHostException e){
					hostIpHash = hostHash;
				}
				ipCount = hostsIpMap.get(hostIpHash);
				if (ipCount==null)
					hostsIpMap.put(hostIpHash,1);
				else if (ipCount>=urlsPerServerPerRun){
					return false;
				}else {
					ipCount++;
					hostsIpMap.put(hostIpHash, ipCount);
				}		
				//LOGGER.info(hostIpHash + " " + url + " " + datum.getScore() + " " + datum.getLastStatus());
				_numSelected++;
				return true;
			} else if (status == UrlStatus.FETCHED){
				if (count==null)
					hostsMap.put(hostHash, 1);
				else {
					count++;
					hostsMap.put(hostHash, count);					
				}
			}
			return false;
		}
	}
	
	public static void resetCounters() {
		hostsMap = new HashMap<Integer,Integer>();
		hostsIpMap = new HashMap<Integer,Integer>();
		_numSelected = 0;
	}
	
	//Custom comparators. These enable grouping the URL list by Status (so that Fetched come first in order
	//for host-occurency hash map to be filled correctly. Everything else that has a status that belongs
	//in statusSet, is ordered by Score
	private static class StatusComparator implements Comparator<String>, Serializable {

		private static final long serialVersionUID = -5250304580989068398L;

		@Override
		public int compare(String o1, String o2) {
			if (o1.equals("FETCHED") && o2.equals("FETCHED")) return 0;
			else if (o1.equals("FETCHED")) return -1;
			else if (o2.equals("FETCHED")) return 1;
			
			if (statusSet.contains(o1) && statusSet.contains(o2)) return 0;
			else return o1.compareTo(o2);			
		}		
	}
	private static class ScoreComparator implements Comparator<Double>, Serializable {

		private static final long serialVersionUID = -1057396789369463851L;

		@Override
		public int compare(Double o1, Double o2) {			
			return o2.compareTo(o1);
		}		
	}
	
	public Flow createFlow(Path curWorkingDirPath, Path crawlDbPath, UserAgent userAgent, FetcherPolicy fetcherPolicy,
			BaseUrlFilter urlFilter, String[] classes, ArrayList<String[]> topic, double thres,int min_uniq_terms,
			int max_depth, int maxThreads, boolean debug, boolean keepBoiler, String language,boolean widen) throws Throwable {
		//int maxThreads = options.getThreads();
		//boolean debug = options.isDebug();
		//boolean keepBoiler = options.keepBoiler();
		//String language = options.getLanguage();
		JobConf conf = SimpleCrawlHFS.conf;
		//JobConf conf = new JobConf();
		//conf.setJarByClass(SimpleCrawlHFS.class);
		//conf.setQuietMode(true);
		//conf.set("hadoop.tmp.dir", "/tmp/hadoop-temp");
		int numReducers = conf.getNumReduceTasks() * HadoopUtils.getTaskTrackers(conf);
		Properties props = HadoopUtils.getDefaultProperties(SimpleCrawlHFSWorkflow.class, debug, conf);
		FileSystem fs = curWorkingDirPath.getFileSystem(conf);
		
		if (!fs.exists(crawlDbPath)) {
			throw new IllegalStateException(String.format("Input directory %s doesn't exist", crawlDbPath));
		}
		//Setting up the input source and the input pipe
		Tap inputSource = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), crawlDbPath.toString());
		//The import pipe contains ALL the urls that the crawler has processed since it started
		//(fetched, unfetched, links etc). Using the custom comparators, these are grouped and sorted
		//first by their STATUS (all fetched URLs will be first in order for the occurencies hashmaps
		//to be filled correctly; these hashmaps will count how many URLs have been stored from each
		//web domain in order to enforce URLs-per-webdomain restrictions). After all fetched URLs have
		//been counted, all the rest that have STATUS that is acceptable for crawling are sorted by
		//their score. 		
		Pipe importPipe = new Pipe("url importer");
		Fields f = new Fields(CrawlDbDatum.LAST_STATUS_FIELD).append(new Fields(CrawlDbDatum.SCORE));
		StatusComparator statComp = new StatusComparator();
		ScoreComparator scoreComp = new ScoreComparator();
		f.setComparator(CrawlDbDatum.LAST_STATUS_FIELD, statComp);
		f.setComparator(CrawlDbDatum.SCORE, scoreComp);		
		importPipe = new GroupBy(importPipe,f);
		//The import pipe is splitted in 2 pipes, one for the next batch of urls to fetch
		//and one for all the rest. The SplitFetchedUnfetchedCrawlDatums will select BUFFER_SIZE
		//URLs from the sorted input source by checking that: a) their web domain does not have
		//an occurrence higher than the thresholds defined in the config and b) their STATUS is one
		//of those defined in statusSet. 
		SplitterAssembly splitter = new SplitterAssembly(importPipe, new SplitFetchedUnfetchedCrawlDatums());
		//finishedDatumsFromDb will represent ALL the URLs from the input source and urlsToFetchPipe
		//will represent the selected BUFFER_SIZE URLs that are to be fetched.
		Pipe finishedDatumsFromDb = new Pipe("finished urls", importPipe);
		Pipe urlsToFetchPipe = new Pipe("urls to Fetch", splitter.getLHSPipe());
		urlsToFetchPipe = new Each(urlsToFetchPipe, new CreateUrlDatumFromCrawlDbFunction()); 
		//Setting up all the sinks (each pipe that will write data is connected to a sink, a "path"
		//for writing data in hadoop terms.
		Path outCrawlDbPath = new Path(curWorkingDirPath, CrawlConfig.CRAWLDB_SUBDIR_NAME);
		Tap loopCrawldbSink = new Hfs(new SequenceFile(CrawlDbDatum.FIELDS), outCrawlDbPath.toString());
		Path contentDirPath = new Path(curWorkingDirPath, CrawlConfig.CONTENT_SUBDIR_NAME);
		Tap contentSink = new Hfs(new SequenceFile(FetchedDatum.FIELDS), contentDirPath.toString());
		Path parseDirPath = new Path(curWorkingDirPath, CrawlConfig.PARSE_SUBDIR_NAME);
		Tap parseSink = new Hfs(new SequenceFile(ExtendedParsedDatum.FIELDS), parseDirPath.toString());
		Path classifierDirPath = new Path(curWorkingDirPath, CrawlConfig.CLASSIFIER_SUBDIR_NAME);
		Tap classifierSink = new Hfs(new SequenceFile(ClassifierDatum.FIELDS), classifierDirPath.toString());

		// Create the sub-assembly that runs the fetch job                
		BaseFetcher fetcher = new SimpleHttpFetcher(maxThreads, fetcherPolicy, userAgent);
		
		((SimpleHttpFetcher) fetcher).setConnectionTimeout(SimpleCrawlHFS.config.getInt("fetcher.connection_timeout.value"));
		((SimpleHttpFetcher) fetcher).setSocketTimeout(SimpleCrawlHFS.config.getInt("fetcher.socket_timeout.value"));
		((SimpleHttpFetcher) fetcher).setMaxRetryCount(SimpleCrawlHFS.config.getInt("fetcher.max_retry_count.value"));
		BaseScoreGenerator scorer = new FixedScoreGenerator();
		FetchPipe fetchPipe = new FetchPipe(urlsToFetchPipe, scorer, fetcher, RobotUtils.createFetcher(fetcher),
				new RobotRulesParser(), new DefaultFetchJobPolicy(fetcherPolicy.getMaxRequestsPerConnection(), 
						SimpleCrawlHFS.config.getInt("fetcher.max_requests_per_host_per_run.value"), 
						fetcherPolicy.getCrawlDelay()), numReducers);        
		//The fetch pipe returns data in 2 different pipes, one contains the content
		//of downloaded pages and one that contains the status of all the URLs that
		//were fed to the fetcher. contentPipe will handle the content of the fetched pages.
		Pipe contentPipe = new Pipe("content pipe", fetchPipe.getContentTailPipe());        
		//contentPipe is parsed. Metadata, content and links are ectracted. Content is
		//cleaned using Boilerpipe.
		ExtendedParsePipe parsePipe = new ExtendedParsePipe(contentPipe, new SimpleNoLinksParser(keepBoiler));
		//The results from the parser are forwarded to the classifier. The classifier
		//will score the content of each fetched page. It will also score all links
		//based on the score of the page they came from, the anchor text and the surrounding
		//text.
		ClassifierPipe classifyPipe = new ClassifierPipe(parsePipe.getTailPipe(),new Classifier(language,classes, topic, thres,keepBoiler,min_uniq_terms, max_depth ));
		Pipe urlsFromClassifier = new Pipe("urls from classifier", classifyPipe.getClassifierTailPipe());
		urlsFromClassifier = new Each(urlsFromClassifier, new SelectUrlOnlyFunction());
		//The classifier's and parser's results are forwarded to their final pipes for writing
		Pipe finalContentPipe = new CoGroup(contentPipe, new Fields(ExtendedUrlDatum.URL_FN),
				urlsFromClassifier, new Fields(ExtendedUrlDatum.URL_FN),FetchedDatum.FIELDS.append(new Fields("url")) ,new RightJoin());
		Pipe finalParsePipe = new CoGroup(parsePipe, new Fields(ExtendedUrlDatum.URL_FN),
				urlsFromClassifier, new Fields(ExtendedUrlDatum.URL_FN),ExtendedParsedDatum.FIELDS.append(new Fields("url")) ,new RightJoin());
		//The links scored by the classifier are handled be the urlFromOutlinksPipe
		Pipe urlFromOutlinksPipe = new Pipe("url from outlinks", classifyPipe.getScoredLinksTailPipe());
		//Outlinks are filtered and normalized
		urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new ExtendedUrlFilter(urlFilter));
		urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new ExtendedNormalizeUrlFunction(new SimpleUrlNormalizer()));
		
		//The second pipe returned from the fetcher, is assigned to urlFromFetchPipe.
		Pipe urlFromFetchPipe = new Pipe("fetched pipe", fetchPipe.getStatusTailPipe());
		//The URLs the Fetcher attempted to fetch are joined with the
		//URLs the classifier managed to score in order to get a pipe
		//with the BUFFER_SIZE URLs and their updated status and scores
		Fields statusFields = StatusDatum.FIELDS;
		Fields classifierFields = ClassifierDatum.FIELDS;
		classifierFields = classifierFields.rename(new Fields(ClassifierDatum.URL_FN), new Fields("classifier_url"));
		classifierFields = classifierFields.rename(new Fields(ClassifierDatum.PAYLOAD_FN), new Fields("classifier_payload"));        
		urlFromFetchPipe = new CoGroup(urlFromFetchPipe,new Fields(ExtendedUrlDatum.URL_FN),
				classifyPipe.getClassifierTailPipe(),new Fields(ExtendedUrlDatum.URL_FN),
				statusFields.append(classifierFields), new LeftJoin());
		urlFromFetchPipe = new Each(urlFromFetchPipe, new CreateUrlDatumFromStatusFunction());
		//Finally, these URLs as well as the extracted links are converted to CrawlDbDatums. 
		//Then, we add/update these 2 pipes to the original pipe containing the whole URL
		//collection when we started this run and we make it unique. The result is the final URL collection that
		//must be stored as a result of this run.
		urlFromFetchPipe = new Each(urlFromFetchPipe, new CreateCrawlDbDatumFromUrlFunction());
		//if (!widen)
			urlFromOutlinksPipe = new Each(urlFromOutlinksPipe,new CreateCrawlDbDatumFromUrlFunction());
		//else 
		//	urlFromOutlinksPipe = new Each(urlFromOutlinksPipe,new GwidenToCrawlDbFunction());
		Pipe finishedDatums = new GroupBy(Pipe.pipes(finishedDatumsFromDb,urlFromFetchPipe, urlFromOutlinksPipe), new Fields(CrawlDbDatum.URL_FIELD));
		finishedDatums = new Every(finishedDatums, new MakeDistinctCrawlDbFunction(),Fields.RESULTS);
		
		// Create the output map that connects each tail pipe to the appropriate sink.
		Map<String, Tap> sinkMap = new HashMap<String, Tap>();
		sinkMap.put(finalContentPipe.getName(), contentSink);
		sinkMap.put(finalParsePipe.getName(), parseSink);
		sinkMap.put(ClassifierPipe.CLASSIFIER_PIPE_NAME, classifierSink);
		sinkMap.put(finishedDatums.getName(), loopCrawldbSink);

		// Finally we can run it.
		FlowConnector flowConnector = new FlowConnector(props);
		
		return flowConnector.connect(inputSource, sinkMap/*, statusOutputPipe*/, finalContentPipe, finalParsePipe,
				classifyPipe.getClassifierTailPipe(), finishedDatums);
	}

}
