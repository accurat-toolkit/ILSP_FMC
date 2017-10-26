package gr.ilsp.fmc.parser;







import gr.ilsp.fmc.classifier.Classifier;
import gr.ilsp.fmc.datums.ClassifierDatum;
import gr.ilsp.fmc.datums.CrawlDbDatum;
import gr.ilsp.fmc.datums.ExtendedParsedDatum;
import gr.ilsp.fmc.datums.ExtendedUrlDatum;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

import com.bixolabs.cascading.LoggingFlowProcess;
import com.bixolabs.cascading.LoggingFlowReporter;
import com.bixolabs.cascading.NullContext;


public class ScoreLinks extends BaseOperation<NullContext> implements Function<NullContext> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 864053504476981356L;
	private Classifier _classifier = null;
	private transient LoggingFlowProcess _flowProcess;
	private enum ScoreLinksCounters {
		SCORING_LINKS_NUMBER, SCORING_LINKS_TIME, SCORING_LINKS_TUNNEL_REJECTED
	}
	public ScoreLinks(Classifier classifier) {
		super(ExtendedUrlDatum.FIELDS);
		_classifier = classifier;        
	}

	@Override
	public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
		super.prepare(process, operationCall);
		_flowProcess = new LoggingFlowProcess((HadoopFlowProcess)process);
		_flowProcess.addReporter(new LoggingFlowReporter());
	}

	@Override
	public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
		//LOGGER.info("Ending outlink scoring");
		_flowProcess.dumpCounters();
		super.cleanup(process, operationCall);
	}

	@Override
	public void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
		long time = System.currentTimeMillis();
		ExtendedUrlDatum resultDatum = null;
		ExtendedParsedDatum datum = null;

		datum = new ExtendedParsedDatum(funcCall.getArguments());
		ExtendedOutlink[] outlinks = datum.getOutlinks();
		double score = datum.getTupleEntry().getDouble(ClassifierDatum.TOTABSCORE) / outlinks.length;
		int crawlDepth = (Integer) datum.getPayloadValue(CrawlDbDatum.CRAWL_DEPTH);
		if (score>0.0) crawlDepth = -1;		
		if (crawlDepth<_classifier.getMaxDepth()) {
			datum.setPayloadValue(CrawlDbDatum.CRAWL_DEPTH, crawlDepth + 1);        
			TupleEntryCollector collector = funcCall.getOutputCollector();
			//ExtendedUrlDatum[] datums = new ExtendedUrlDatum[outlinks.length];
			for (ExtendedOutlink outlink : outlinks) {        	
				String linktext = outlink.getAnchor() + " " + outlink.getSurroundText();
				double linkScore = 0.0;
				if (_classifier.getTopic()!=null) linkScore = _classifier.rankLink(linktext);
				String url = outlink.getToUrl();
				if (url==null) continue;
				url = url.replaceAll("[\n\r]", "");            
				resultDatum = new ExtendedUrlDatum(url);                    
				resultDatum.setPayload(datum.getPayload());           
				resultDatum.setScore(score + linkScore);    
				collector.add(resultDatum.getTuple());
				_flowProcess.increment(ScoreLinksCounters.SCORING_LINKS_NUMBER, 1);
			}
		} else _flowProcess.increment(ScoreLinksCounters.SCORING_LINKS_TUNNEL_REJECTED,outlinks.length);
		_flowProcess.increment(ScoreLinksCounters.SCORING_LINKS_TIME, (int)(System.currentTimeMillis()-time));
	}




}
