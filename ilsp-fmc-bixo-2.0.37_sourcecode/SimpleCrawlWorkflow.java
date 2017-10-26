package gr.ilsp.fmc.workflows;

import gr.ilsp.fmc.classifier.Classifier;
import gr.ilsp.fmc.datums.ClassifierDatum;
import gr.ilsp.fmc.datums.CrawlDbDatum;
import gr.ilsp.fmc.datums.ExtendedParsedDatum;
import gr.ilsp.fmc.datums.ExtendedUrlDatum;
import gr.ilsp.fmc.main.SimpleCrawl;
import gr.ilsp.fmc.mysql.MYSQLTapFactory;
import gr.ilsp.fmc.operations.CreateCrawlDbDatumFromUrlFunction;
import gr.ilsp.fmc.operations.CreateUrlDatumFromStatusFunction;
import gr.ilsp.fmc.operations.ExtendedNormalizeUrlFunction;
import gr.ilsp.fmc.operations.SelectFetchedOnlyFunction;
import gr.ilsp.fmc.parser.SimpleNoLinksParser;
import gr.ilsp.fmc.pipes.ClassifierPipe;
import gr.ilsp.fmc.pipes.ExtendedParsePipe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import bixo.config.FetcherPolicy;
import bixo.config.UserAgent;
import bixo.datum.FetchedDatum;
import bixo.datum.StatusDatum;
import bixo.datum.UrlStatus;
import bixo.fetcher.BaseFetcher;
import bixo.fetcher.SimpleHttpFetcher;
import bixo.operations.BaseScoreGenerator;
import bixo.operations.FixedScoreGenerator;
import bixo.operations.UrlFilter;
import bixo.pipes.FetchPipe;
import bixo.urls.BaseUrlFilter;
import bixo.urls.SimpleUrlNormalizer;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.LeftJoin;
import cascading.pipe.cogroup.RightJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.bixolabs.cascading.BaseSplitter;
import com.bixolabs.cascading.HadoopUtils;
import com.bixolabs.cascading.NullContext;
import com.bixolabs.cascading.SplitterAssembly;

@SuppressWarnings("deprecation")
public class SimpleCrawlWorkflow {
    private static final Logger LOGGER = Logger.getLogger(SimpleCrawlWorkflow.class);
    
    @SuppressWarnings("serial")
    private static class SplitFetchedUnfetchedCrawlDatums extends BaseSplitter {
    	private long _numSelected = 0;
        @Override
        public String getLHSName() {
            return "fetched unfetched UrlDatums";
        }

        @Override
        // LHS represents unfetched tuples
        public boolean isLHS(TupleEntry tupleEntry) {
            CrawlDbDatum datum = new CrawlDbDatum(tupleEntry);
            UrlStatus status = datum.getLastStatus();
            if ((_numSelected<256) && (status == UrlStatus.UNFETCHED
                || status == UrlStatus.SKIPPED_DEFERRED
                || status == UrlStatus.SKIPPED_BY_SCORER
                || status == UrlStatus.SKIPPED_BY_SCORE
                || status == UrlStatus.SKIPPED_TIME_LIMIT
                || status == UrlStatus.SKIPPED_INTERRUPTED
                || status == UrlStatus.SKIPPED_INEFFICIENT
                || status == UrlStatus.ABORTED_SLOW_RESPONSE
                || status == UrlStatus.ERROR_IOEXCEPTION)) {
            	_numSelected++;
                return true;
            }
            return false;
        }
    }
    @SuppressWarnings("serial")
    private static class CreateUrlDatumFromCrawlDbFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        public CreateUrlDatumFromCrawlDbFunction() {
            super(ExtendedUrlDatum.FIELDS);
        }

        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Starting creation of URLs from crawldb");
        }

        @Override
        public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Ending creation of URLs from crawldb");
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
            CrawlDbDatum datum = new CrawlDbDatum(funcCall.getArguments());
            ExtendedUrlDatum urlDatum = new ExtendedUrlDatum(datum.getUrl());
            urlDatum.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, datum.getLastFetched());
            urlDatum.setPayloadValue(CrawlDbDatum.LAST_UPDATED_FIELD, datum.getLastUpdated());
            urlDatum.setPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD, datum.getLastStatus().name());
            urlDatum.setPayloadValue(CrawlDbDatum.CRAWL_DEPTH, datum.getCrawlDepth());
            urlDatum.setScore(datum.getScore());
            funcCall.getOutputCollector().add(urlDatum.getTuple());
        }
    }
    @SuppressWarnings("serial")
    private static class DistinctMergedPipeFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        public DistinctMergedPipeFunction() {
            super(ExtendedUrlDatum.FIELDS);
        }

        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Starting processing outlinks to find only new ones");
        }

        @Override
        public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Ending processing outlinks to find new ones.");
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
            ExtendedUrlDatum urlDatum = new ExtendedUrlDatum(funcCall.getArguments());
            ExtendedUrlDatum datum = new ExtendedUrlDatum(urlDatum.getUrl());
            if (urlDatum.getTupleEntry().get(3)==null) {
            	datum.setPayload(urlDatum.getPayload());
            	datum.setScore(urlDatum.getScore());
            	funcCall.getOutputCollector().add(datum.getTuple());
            }
        }
    }
    @SuppressWarnings("serial")
    private static class SelectUrlOnlyFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    	
        public SelectUrlOnlyFunction() {  
        	super(new Fields(ExtendedUrlDatum.URL_FN));
        }

        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
           // ExtendedUrlDatum urlDatum = new ExtendedUrlDatum(funcCall.getArguments());
            String url = funcCall.getArguments().get(ExtendedUrlDatum.URL_FN).toString();
            Tuple tup = new Tuple(url);
            //ExtendedUrlDatum datum = new ExtendedUrlDatum(urlDatum.getUrl());
            //if (urlDatum.getTupleEntry().get(4)==null) {
            //	datum.setPayload(urlDatum.getPayload());
            //	datum.setScore(urlDatum.getScore());
            	funcCall.getOutputCollector().add(tup);
            //}
        }
    }
    @SuppressWarnings("serial")
    private static class MakeDistinctFunction extends BaseOperation<NullContext> implements Buffer<NullContext> {
        
        private long _numSelected = 0;
        
        public MakeDistinctFunction() {
            super(ExtendedUrlDatum.FIELDS);
        }
        
        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Starting processing links to make unique");
        }

        @Override
        public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("Ending making unique links - dropped " + _numSelected + " urls");
        }

        @Override
        public void operate(FlowProcess process, BufferCall<NullContext> bufferCall) {
            ExtendedUrlDatum datum = null;
            ExtendedUrlDatum nowDatum = null;
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();  
            double avgScore = 0.0;
            int iterations = 0;
            while (iter.hasNext()) {
            	iterations++;
            	nowDatum = new ExtendedUrlDatum(iter.next());
                if (datum == null) {
                	datum = new ExtendedUrlDatum(nowDatum.getUrl());
                	datum.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, nowDatum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD));
                	datum.setPayloadValue(CrawlDbDatum.LAST_UPDATED_FIELD, nowDatum.getPayloadValue(CrawlDbDatum.LAST_UPDATED_FIELD));
                	datum.setPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD, nowDatum.getPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD));
                	datum.setPayloadValue(CrawlDbDatum.CRAWL_DEPTH, nowDatum.getPayloadValue(CrawlDbDatum.CRAWL_DEPTH));
                	//datum.setScore(nowDatum.getScore());
                	avgScore += nowDatum.getScore();
                } else {
                	avgScore += nowDatum.getScore();
                	//datum = null;
                	_numSelected++;
                	//break;
                }
            }
            avgScore = avgScore/iterations;
            datum.setScore(avgScore);
            if (datum != null) {
            	bufferCall.getOutputCollector().add(datum.getTuple());
                //_numSelected++;
            }
        }

    }
    /*@SuppressWarnings("serial")//USE THIS FOR DEBUGGING PURPOSES ONLY
    private static class PrintDatumsFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        public PrintDatumsFunction() {
            super(ExtendedUrlDatum.FIELDS);
        }

        @Override
        public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("!!!!!!!!!!!!PRINTING DATUMS");
        }

        @Override
        public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
            LOGGER.info("!!!!!!!!!!!!ENDING PRINTING DATUMS");
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
            ExtendedUrlDatum datum = new ExtendedUrlDatum(funcCall.getArguments());
            System.out.println(datum.getUrl()+ " " + datum.getScore());
            funcCall.getOutputCollector().add(datum.getTuple());
        }
    }*/
    

    
    public static Flow createFlow(Path inputDir, Path curLoopDirPath, UserAgent userAgent, FetcherPolicy fetcherPolicy,
                    BaseUrlFilter urlFilter, int maxThreads, boolean debug, String persistentDbLocation, String dbname, String language, 
                    String[] classes, ArrayList<String[]> topic, double thres, int min_uniq_terms, int max_depth) throws Throwable {
        //JobConf conf = HadoopUtils.getDefaultJobConf(CrawlConfig.CRAWL_STACKSIZE_KB);
    	JobConf conf = new JobConf();
        conf.setJarByClass(SimpleCrawl.class);
        conf.setQuietMode(true);
        int numReducers = conf.getNumReduceTasks() * HadoopUtils.getTaskTrackers(conf);
        FileSystem fs = curLoopDirPath.getFileSystem(conf);

        if (!fs.exists(inputDir)) {
            throw new IllegalStateException(String.format("Input directory %s doesn't exist", inputDir));
        }
        //Setting up the input source and the input pipe
        Tap inputSource = MYSQLTapFactory.createUrlsSourceJDBCTap(persistentDbLocation,dbname);
        Pipe importPipe = new Pipe("url importer");        
        
        //The import pipe is splitted in 2 pipes, one for the next batch of urls to fetch
        //and one for all the rest
        SplitterAssembly splitter = new SplitterAssembly(importPipe, new SplitFetchedUnfetchedCrawlDatums());
        Pipe finishedDatumsFromDb = new Pipe("finished urls", splitter.getRHSPipe());
        Pipe urlsToFetchPipe = new Pipe("urls to Fetch", splitter.getLHSPipe());
        urlsToFetchPipe = new Each(urlsToFetchPipe, new CreateUrlDatumFromCrawlDbFunction());
        
        //Setting up all the sinks
        Tap contentSink = MYSQLTapFactory.createContentSinkJDBCTap(persistentDbLocation,dbname);
        Tap parseSink = MYSQLTapFactory.createParseSinkJDBCTap(persistentDbLocation,dbname);
        Tap classifierSink = MYSQLTapFactory.createClassifierSinkJDBCTap(persistentDbLocation,dbname);
        //Tap statusSink = MYSQLTapFactory.createStatusSinkJDBCTap(persistentDbLocation);
        Tap urlSink = MYSQLTapFactory.createUrlsSinkJDBCTap(persistentDbLocation,dbname);

        // Create the sub-assembly that runs the fetch job        
        BaseFetcher fetcher = new SimpleHttpFetcher(maxThreads, fetcherPolicy, userAgent);
        BaseScoreGenerator scorer = new FixedScoreGenerator();//This is used only to score URLs, in reality it assigns 1.0 to all
        FetchPipe fetchPipe = new FetchPipe(urlsToFetchPipe, scorer, fetcher, numReducers);
        //The fetch pipe returns data in 2 different pipes, one contains the content
        //of downloaded pages and one that contains the status of all the URLs that
        //were fed to the fetcher. We assign these to contentPipe and statusOutputPipe
        Pipe contentPipe = new Pipe("content pipe", fetchPipe.getContentTailPipe());
        //StatusOutputPipe statusOutputPipe = new StatusOutputPipe(fetchPipe.getStatusTailPipe(),new StatusFilter());              
        //contentPipe is parsed
        ExtendedParsePipe parsePipe = new ExtendedParsePipe(contentPipe, new SimpleNoLinksParser());
        //The results from the parser are forwared to the classifier
        ClassifierPipe classifyPipe = new ClassifierPipe(parsePipe.getTailPipe(),new Classifier(language,classes, topic, thres, false,min_uniq_terms,max_depth));
        Pipe urlsFromClassifier = new Pipe("urls from classifier", classifyPipe.getClassifierTailPipe());
        urlsFromClassifier = new Each(urlsFromClassifier, new SelectUrlOnlyFunction());
        Pipe finalContentPipe = new CoGroup(contentPipe, new Fields(ExtendedUrlDatum.URL_FN),
        		urlsFromClassifier, new Fields(ExtendedUrlDatum.URL_FN),FetchedDatum.FIELDS.append(new Fields("url")) ,new RightJoin());
        Pipe finalParsePipe = new CoGroup(parsePipe, new Fields(ExtendedUrlDatum.URL_FN),
        		urlsFromClassifier, new Fields(ExtendedUrlDatum.URL_FN),ExtendedParsedDatum.FIELDS.append(new Fields("url")) ,new RightJoin());
        //The classifier return one pipe with all the scored extracted links
        Pipe urlFromOutlinksPipe = new Pipe("url from outlinks", classifyPipe.getScoredLinksTailPipe());
        //Outlinks are filtered, normalized and made unique
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new UrlFilter(urlFilter));
        urlFromOutlinksPipe = new Each(urlFromOutlinksPipe, new ExtendedNormalizeUrlFunction(new SimpleUrlNormalizer()));
        urlFromOutlinksPipe = new GroupBy(urlFromOutlinksPipe, new Fields(ExtendedUrlDatum.URL_FN));
        urlFromOutlinksPipe = new Every(urlFromOutlinksPipe, new MakeDistinctFunction(),Fields.RESULTS);        
        
        //The second pipe returned from the fetcher, is assigned to urlFromFetchPipe.
        Pipe urlFromFetchPipe = new Pipe("fetched pipe", fetchPipe.getStatusTailPipe());
        //We perform an Inner Join between the status pipe returned from the fetcher and the
        //classifier results in order to be able to perform an update
        Fields statusFields = StatusDatum.FIELDS;
        Fields classifierFields = ClassifierDatum.FIELDS;
        classifierFields = classifierFields.rename(new Fields(ClassifierDatum.URL_FN), new Fields("classifier_url"));
        classifierFields = classifierFields.rename(new Fields(ClassifierDatum.PAYLOAD_FN), new Fields("classifier_payload"));        
        urlFromFetchPipe = new CoGroup(urlFromFetchPipe,new Fields(ExtendedUrlDatum.URL_FN),
        		classifyPipe.getClassifierTailPipe(),new Fields(ExtendedUrlDatum.URL_FN),
        		statusFields.append(classifierFields), new LeftJoin());
        urlFromFetchPipe = new Each(urlFromFetchPipe, new CreateUrlDatumFromStatusFunction());
        //The resulting pipe contains all the URLs that were fed to the fetcher along with
        //their scores, or the reason the fetch failed. We use this pipe with a special sink
        //to update the database.
        Pipe updatePipe = new Each(urlFromFetchPipe, new CreateCrawlDbDatumFromUrlFunction());
        updatePipe = new Each(updatePipe,new SelectFetchedOnlyFunction());
        Tap urlUpdateSink = MYSQLTapFactory.createUrlUpdateSink(persistentDbLocation,dbname);
        //Finally, we must get all the new links by doing a left join between the outlinks pipe
        //and the grouped pipes of the fetched status pipe and all the rest pipe we created when started.
        Pipe finishedDatums = new Each(finishedDatumsFromDb, new CreateUrlDatumFromCrawlDbFunction());
        finishedDatums = new Each(finishedDatums, new SelectUrlOnlyFunction());
        urlFromFetchPipe = new Each(urlFromFetchPipe, new SelectUrlOnlyFunction());
        finishedDatums = new GroupBy(Pipe.pipes(finishedDatums, urlFromFetchPipe), new Fields(ExtendedUrlDatum.URL_FN));
        Pipe urlPipe = new CoGroup("url pipe", urlFromOutlinksPipe,new Fields(ExtendedUrlDatum.URL_FN),
        		finishedDatums,new Fields(ExtendedUrlDatum.URL_FN),
                		ExtendedUrlDatum.FIELDS.append(new Fields("url")), new LeftJoin());
        //After the left join, all we need is to keep all tuples that only belong to the outlinks
        //pipe and don't have values from the other pipes, so that we know that they are new.
        //To accomplish this we use the DistinctMergedPipeFunction
        urlPipe = new Each(urlPipe, new DistinctMergedPipeFunction());
        //Pipe outputPipe = new Pipe ("output pipe");
        Pipe outputPipe = new Each(urlPipe, new CreateCrawlDbDatumFromUrlFunction());

        // Create the output map that connects each tail pipe to the appropriate sink.
        Map<String, Tap> sinkMap = new HashMap<String, Tap>();
        //sinkMap.put(StatusOutputPipe.STATUSOUTPUT_PIPE_NAME, statusSink);
        sinkMap.put(finalContentPipe.getName(), contentSink);
        sinkMap.put(updatePipe.getName(), urlUpdateSink);
        sinkMap.put(finalParsePipe.getName(), parseSink);
        sinkMap.put(ClassifierPipe.CLASSIFIER_PIPE_NAME, classifierSink);
        sinkMap.put(outputPipe.getName(), urlSink);
        
        // Finally we can run it.
        FlowConnector flowConnector = new FlowConnector(HadoopUtils.getDefaultProperties(SimpleCrawl.class, debug, conf));
        return flowConnector.connect(inputSource, sinkMap/*, statusOutputPipe*/, finalContentPipe, finalParsePipe,
        		classifyPipe.getClassifierTailPipe(),updatePipe, outputPipe);
            
    }

}
