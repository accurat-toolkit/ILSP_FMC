package gr.ilsp.fmc.operations;


import gr.ilsp.fmc.datums.CrawlDbDatum;
import gr.ilsp.fmc.datums.ExtendedUrlDatum;
import bixo.datum.UrlStatus;
import cascading.flow.FlowProcess;

import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;



import com.bixolabs.cascading.NullContext;


public class GwidenToCrawlDbFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 394967893319110139L;
	private int counter = 0;
	//private transient LoggingFlowProcess _flowProcess;
    //private enum CrawlDbConversionCounters{
	//	CRAWL_DATUMS_CREATED, CRAWL_DATUMS_CREATION_TIME
    //}

    public GwidenToCrawlDbFunction() {
        super(CrawlDbDatum.FIELDS);
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
    	super.prepare(flowProcess, operationCall);
        //_flowProcess = new LoggingFlowProcess((HadoopFlowProcess)flowProcess);
        //_flowProcess.addReporter(new LoggingFlowReporter());
    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
    	//_flowProcess.dumpCounters();
        super.cleanup(flowProcess, operationCall);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
    	if (counter==0)
    		System.out.println("Inside operate of Gwiden");
    	counter++;
    	//long time = System.currentTimeMillis();
        ExtendedUrlDatum datum = new ExtendedUrlDatum(funcCall.getArguments());        
        Long lastFetched = (Long) datum.getPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD);
        Long lastUpdated = (Long) datum.getPayloadValue(CrawlDbDatum.LAST_UPDATED_FIELD);
        UrlStatus status = UrlStatus.valueOf((String) (datum.getPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD)));        
        Integer crawlDepth = (Integer) datum.getPayloadValue(CrawlDbDatum.CRAWL_DEPTH);
        Double score = datum.getScore();
        CrawlDbDatum crawldbDatum = new CrawlDbDatum(datum.getUrl(), lastFetched, lastUpdated, status, crawlDepth, score);                
        funcCall.getOutputCollector().add(crawldbDatum.getTuple());        
        //_flowProcess.increment(CrawlDbConversionCounters.CRAWL_DATUMS_CREATED, 1);
        //_flowProcess.increment(CrawlDbConversionCounters.CRAWL_DATUMS_CREATION_TIME, (int)(System.currentTimeMillis()-time));        
    }
}
