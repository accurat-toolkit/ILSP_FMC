package gr.ilsp.fmc.operations;

import gr.ilsp.fmc.datums.CrawlDbDatum;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntry;

import com.bixolabs.cascading.LoggingFlowProcess;
import com.bixolabs.cascading.LoggingFlowReporter;
import com.bixolabs.cascading.NullContext;

public class MakeDistinctCrawlDbFunction extends BaseOperation<NullContext> implements Buffer<NullContext> {

	private static final long serialVersionUID = -3251085418755783557L;
	private transient LoggingFlowProcess _flowProcess;
	private enum DistinctFunctionCounters{
		MAKE_DISTINCT_TIME, MAKE_DISTINCT_URLS_PROCESSED, MAKE_DISTINCT_URLS_UNIQUE

	}
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
	
	public MakeDistinctCrawlDbFunction() {
		super(CrawlDbDatum.FIELDS);
	}

	@Override
	public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {            
		super.prepare(flowProcess, operationCall);
		_flowProcess = new LoggingFlowProcess((HadoopFlowProcess)flowProcess);
		_flowProcess.addReporter(new LoggingFlowReporter());
	}

	@Override
	public void cleanup(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {            
		_flowProcess.dumpCounters();
		super.cleanup(flowProcess, operationCall);
	}

	@Override
	public void operate(FlowProcess process, BufferCall<NullContext> bufferCall) {
		long time = System.currentTimeMillis();
		boolean update = true;
		CrawlDbDatum datum = null;
		CrawlDbDatum nowDatum = null;
		Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();  
		double maxScore = 0.0;
		boolean stop = false;
		while (iter.hasNext() && !stop) {
			_flowProcess.increment(DistinctFunctionCounters.MAKE_DISTINCT_URLS_PROCESSED,1);
			nowDatum = new CrawlDbDatum(iter.next());
			double nowScore = nowDatum.getScore();
			if (!statusSet.contains(nowDatum.getLastStatus().name())) {
				update = true;
				stop = true;
			} else 
				if (datum !=null){
				if (datum.getLastUpdated()<=nowDatum.getLastUpdated())
					update = true;
				else
					update = false;
			} else update = true;
			if (update){
				datum = new CrawlDbDatum();
				datum.setUrl(nowDatum.getUrl());
				datum.setLastFetched(nowDatum.getLastFetched());
				datum.setLastStatus(nowDatum.getLastStatus());
				datum.setLastUpdated(nowDatum.getLastUpdated());
				datum.setCrawlDepth(nowDatum.getCrawlDepth());
				maxScore = nowScore>maxScore?nowScore:maxScore;
			}
		}
		datum.setScore(maxScore);
		if (datum != null) {
			_flowProcess.increment(DistinctFunctionCounters.MAKE_DISTINCT_URLS_UNIQUE,1);
			bufferCall.getOutputCollector().add(datum.getTuple());
		}
		_flowProcess.increment(DistinctFunctionCounters.MAKE_DISTINCT_TIME, (int)(System.currentTimeMillis()-time)); 
	}

}