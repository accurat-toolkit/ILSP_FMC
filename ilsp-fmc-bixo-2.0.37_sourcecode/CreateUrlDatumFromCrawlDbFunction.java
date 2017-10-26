package gr.ilsp.fmc.operations;

import gr.ilsp.fmc.datums.CrawlDbDatum;
import gr.ilsp.fmc.datums.ExtendedUrlDatum;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;

import com.bixolabs.cascading.LoggingFlowProcess;
import com.bixolabs.cascading.LoggingFlowReporter;
import com.bixolabs.cascading.NullContext;

public class CreateUrlDatumFromCrawlDbFunction extends BaseOperation<NullContext> implements Function<NullContext> {

	private static final long serialVersionUID = -7763027361611903895L;
	private transient LoggingFlowProcess _flowProcess;
	private enum CreateUrlDatumCounters {
		CONVERSION_DOCUMENTS,   
		CONVERSION_TIME       	    
	}
	public CreateUrlDatumFromCrawlDbFunction() {
		super(ExtendedUrlDatum.FIELDS);
	}

	@Override
	public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
		super.prepare(process, operationCall);
		_flowProcess = new LoggingFlowProcess((HadoopFlowProcess)process);
		_flowProcess.addReporter(new LoggingFlowReporter());			
	}

	@Override
	public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
		_flowProcess.dumpCounters();
		super.cleanup(process, operationCall);
	}

	@Override
	public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
		long time = System.currentTimeMillis();
		CrawlDbDatum datum = new CrawlDbDatum(funcCall.getArguments());
		ExtendedUrlDatum urlDatum = new ExtendedUrlDatum(datum.getUrl());
		urlDatum.setPayloadValue(CrawlDbDatum.LAST_FETCHED_FIELD, datum.getLastFetched());
		urlDatum.setPayloadValue(CrawlDbDatum.LAST_UPDATED_FIELD, datum.getLastUpdated());
		urlDatum.setPayloadValue(CrawlDbDatum.LAST_STATUS_FIELD, datum.getLastStatus().name());
		urlDatum.setPayloadValue(CrawlDbDatum.CRAWL_DEPTH, datum.getCrawlDepth());
		urlDatum.setScore(datum.getScore());
		funcCall.getOutputCollector().add(urlDatum.getTuple());
		_flowProcess.increment(CreateUrlDatumCounters.CONVERSION_DOCUMENTS, 1);
		_flowProcess.increment(CreateUrlDatumCounters.CONVERSION_TIME, (int)(System.currentTimeMillis()-time));
	}
}