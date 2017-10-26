/*
 * Copyright (c) 2010 TransPac Software, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
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


public class CreateCrawlDbDatumFromUrlFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 394967893319110139L;
	//private transient LoggingFlowProcess _flowProcess;
    //private enum CrawlDbConversionCounters{
	//	CRAWL_DATUMS_CREATED, CRAWL_DATUMS_CREATION_TIME
    //}

    public CreateCrawlDbDatumFromUrlFunction() {
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
