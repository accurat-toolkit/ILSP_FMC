package gr.ilsp.fmc.operations;

import gr.ilsp.fmc.datums.CrawlDbDatum;

import org.apache.log4j.Logger;

import bixo.datum.UrlStatus;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class SelectFetchedOnlyFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = Logger.getLogger(SelectFetchedOnlyFunction.class);

    private long _numCreated;

    public SelectFetchedOnlyFunction() {
        super(CrawlDbDatum.FIELDS);
    }

    @Override
    public void prepare(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Starting selecting fetched urls");
        _numCreated = 0;
    }

    @Override
    public void cleanup(FlowProcess process, OperationCall<NullContext> operationCall) {
        LOGGER.info("Ending selecting fetched urls");
        LOGGER.info("Crawldb datums created : " + _numCreated);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
        CrawlDbDatum datum = new CrawlDbDatum(funcCall.getArguments());        
        if (datum.getLastStatus()!=UrlStatus.UNFETCHED){
        	funcCall.getOutputCollector().add(datum.getTuple());
        	_numCreated++;
        }
    }
}
