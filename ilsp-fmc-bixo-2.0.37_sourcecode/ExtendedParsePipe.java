/*
 * Copyright (c) 1997-2009 101tec Inc.
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
package gr.ilsp.fmc.pipes;



import gr.ilsp.fmc.datums.ExtendedParsedDatum;
import gr.ilsp.fmc.main.SimpleCrawlHFS;
import gr.ilsp.fmc.parser.SimpleNoLinksParser;
import org.apache.log4j.Logger;
import bixo.datum.FetchedDatum;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

import com.bixolabs.cascading.LoggingFlowProcess;
import com.bixolabs.cascading.LoggingFlowReporter;
import com.bixolabs.cascading.NullContext;


public class ExtendedParsePipe extends SubAssembly {
    /**
	 * 
	 */
	private static final long serialVersionUID = -5601927304688260557L;

	private static final Logger LOGGER = Logger.getLogger(ExtendedParsePipe.class);
    
    public static final String PARSE_PIPE_NAME = "parse_pipe";
    
    private enum ExtendedParserCounters {
        PARSER_DOCUMENTS_PARSED,   // successfully parsed a document.
        PARSER_DOCUMENTS_FAILED,    // failed to parse a document
        PARSER_TIME
    }
    private static class ParseFunction extends BaseOperation<NullContext> implements Function<NullContext> {
    	
        /**
		 * 
		 */
		private static final long serialVersionUID = -8494284260038088034L;
		private transient LoggingFlowProcess _flowProcess;
        private SimpleNoLinksParser _parser;

        public ParseFunction(SimpleNoLinksParser parser) {
            super(ExtendedParsedDatum.FIELDS);
            _parser = parser;
        }

        @Override
        public void prepare(FlowProcess flowProcess,
                            OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            _flowProcess = new LoggingFlowProcess((HadoopFlowProcess)flowProcess);
            _flowProcess.addReporter(new LoggingFlowReporter());
        }

        @Override
        public boolean isSafe() {
            // We don't want to get called multiple times for the same tuple
            return false;
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
        	long time = System.currentTimeMillis();
            FetchedDatum fetchedDatum = new FetchedDatum(functionCall.getArguments());
            
            try {
            	ExtendedParsedDatum parseResult = _parser.parse(fetchedDatum);            	
                _flowProcess.increment(ExtendedParserCounters.PARSER_DOCUMENTS_PARSED, 1);
                functionCall.getOutputCollector().add(parseResult.getTuple());
                SimpleCrawlHFS.incrementPagesVisited();
            } catch (Exception e) {
                LOGGER.warn("Error processing " + fetchedDatum.getBaseUrl());
                _flowProcess.increment(ExtendedParserCounters.PARSER_DOCUMENTS_FAILED, 1);
                // TODO KKr - don't lose datums for documents that couldn't be parsed
            } finally {
            	_flowProcess.increment(ExtendedParserCounters.PARSER_TIME, (int)(System.currentTimeMillis()-time));
            }
        }
        
        @Override
        public void cleanup(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            _flowProcess.dumpCounters();
            super.cleanup(flowProcess, operationCall);
        }
    }

    public ExtendedParsePipe(Pipe fetcherPipe) {
        this(fetcherPipe, new SimpleNoLinksParser());
    }
    
    public ExtendedParsePipe(Pipe fetcherPipe, SimpleNoLinksParser parser) {
        Pipe parsePipe = new Pipe(PARSE_PIPE_NAME, fetcherPipe);
        
        //parsePipe = new GroupBy(parsePipe, new Fields(FetchedDatum.NEW_BASE_URL_FN), new Fields(FetchedDatum.URL_FN));
        //parsePipe = new Every(parsePipe, new ParseSetsFunction(), Fields.RESULTS);
        
        ParseFunction parserFunction = new ParseFunction(parser);
        parsePipe = new Each(parsePipe, parserFunction, Fields.RESULTS);
        
        setTails(parsePipe);
    }

    
    /*private static class ParseSetsFunction extends BaseOperation<NullContext> implements Buffer<NullContext> {
        
        *//**
		 * 
		 *//*
		private static final long serialVersionUID = 6343689187646754219L;
		private transient LoggingFlowProcess _flowProcess;
		
        
        public ParseSetsFunction() {
            super(ExtendedParsedDatum.FIELDS);
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
        	FetchedDatum datum = null;
        	Callable<ExtendedParsedDatum> parseCallable = null;
        	//FetchedDatum nowDatum = null;
        	ExecutorService taskExecutor = Executors.newFixedThreadPool(2);;        	
            //Future<ExtendedParsedDatum> task =taskExecutor.submit(linkcallable);
        	Collection<Callable<ExtendedParsedDatum>> tasks = new ArrayList<Callable<ExtendedParsedDatum>>();
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();  
            while (iter.hasNext()) {
            	datum = new FetchedDatum(iter.next().getTuple());
            	parseCallable = new SimpleNoLinksParser(datum);
            	tasks.add(parseCallable);
            }
            List<Future<ExtendedParsedDatum>> results = null;
			try {
				results = taskExecutor.invokeAll(tasks);
				for (Future<ExtendedParsedDatum> f : results){
					bufferCall.getOutputCollector().add(f.get().getTuple());					
					_flowProcess.increment(ExtendedParserCounters.PARSER_DOCUMENTS_PARSED, 1);
				} 
			} catch (InterruptedException e) {
				LOGGER.warn("Failed parsing, " + e.getMessage());
				_flowProcess.increment(ExtendedParserCounters.PARSER_DOCUMENTS_FAILED, 1);
			} catch (ExecutionException e) {
				LOGGER.warn("Failed parsing, " + e.getMessage());
				_flowProcess.increment(ExtendedParserCounters.PARSER_DOCUMENTS_FAILED, 1);
			} finally {		
				taskExecutor.shutdown();
				_flowProcess.increment(ExtendedParserCounters.PARSER_TIME, (int)(System.currentTimeMillis()-time));
			}
			                        
        }

    }*/
    
    
    
    
    
    public Pipe getTailPipe() {
        String[] pipeNames = getTailNames();
        if (pipeNames.length != 1) {
            throw new RuntimeException("Unexpected number of tail pipes!");
        }
        
        if (!pipeNames[0].equals(PARSE_PIPE_NAME)) {
            throw new RuntimeException("Unexpected name for tail pipe");
        }
        
        return getTails()[0];
    }

}
