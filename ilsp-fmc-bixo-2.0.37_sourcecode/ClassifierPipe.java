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

import gr.ilsp.fmc.classifier.Classifier;
import gr.ilsp.fmc.classifier.ClassifierCounters;
import gr.ilsp.fmc.datums.ClassifierDatum;
import gr.ilsp.fmc.datums.ExtendedParsedDatum;
import gr.ilsp.fmc.main.SimpleCrawlHFS;
import gr.ilsp.fmc.parser.ScoreLinks;

import org.apache.log4j.Logger;


import bixo.datum.UrlDatum;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.cogroup.RightJoin;
import cascading.tuple.Fields;

import com.bixolabs.cascading.LoggingFlowProcess;
import com.bixolabs.cascading.LoggingFlowReporter;
import com.bixolabs.cascading.NullContext;


public class ClassifierPipe extends SubAssembly {
    /**
	 * 
	 */
	private static final long serialVersionUID = 916057049645756562L;

	private static final Logger LOGGER = Logger.getLogger(ClassifierPipe.class);
    
    public static final String CLASSIFIER_PIPE_NAME = "classifier_pipe";
    public static final String SCORED_LINKS_PIPE_NAME = "scored_links_pipe";

    private static class ClassifyFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        /**
		 * 
		 */
		private static final long serialVersionUID = -6664107549904861854L;
		private transient LoggingFlowProcess _flowProcess;
        private Classifier _classifier;

        public ClassifyFunction(Classifier classifier) {
            super(ClassifierDatum.FIELDS);
            _classifier = classifier;
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
            // We don"t want to get called multiple times for the same tuple
            return false;
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            long time = System.currentTimeMillis();
        	ExtendedParsedDatum parsedDatum = new ExtendedParsedDatum(functionCall.getArguments());
            
            try {
                ClassifierDatum classifyResult = _classifier.classify(parsedDatum);
                if (classifyResult==null) 
                	_flowProcess.increment(ClassifierCounters.CLASSIFIER_DOCUMENTS_FAILED, 1);
                else {
	                _flowProcess.increment(ClassifierCounters.CLASSIFIER_DOCUMENTS_PASSED, 1);                
	                functionCall.getOutputCollector().add(classifyResult.getTuple());
	                SimpleCrawlHFS.incrementPagesStored();
                }
            } catch (Exception e) {
                LOGGER.warn("Error processing " + parsedDatum.getUrl(), e);
                _flowProcess.increment(ClassifierCounters.CLASSIFIER_DOCUMENTS_ABORTED, 1);
                // TODO KKr - don"t lose datums for documents that couldn"t be parsed
            } finally {
            	_flowProcess.increment(ClassifierCounters.CLASSIFIER_TIME,(int)(System.currentTimeMillis()-time));
            }
        }
        
        @Override
        public void cleanup(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            _flowProcess.dumpCounters();
            super.cleanup(flowProcess, operationCall);
        }
    }

    //public ClassifierPipe(Pipe parsedPipe) {
    //    this(parsedPipe, new Classifier());
    //}
    
    public ClassifierPipe(Pipe parsedPipe, Classifier classifier) {
        Pipe classifierPipe = new Pipe(CLASSIFIER_PIPE_NAME, parsedPipe);

        ClassifyFunction classifyFunction = new ClassifyFunction(classifier);
        classifierPipe = new Each(classifierPipe, classifyFunction, Fields.RESULTS);  
        classifierPipe = new GroupBy(classifierPipe, new Fields(UrlDatum.URL_FN));
        Fields f = new Fields("ClassifierDatum-subclasses", "ClassifierDatum-subscores", "ClassifierDatum-totabscore", "ClassifierDatum-totrelscore", "url1","payload1", "ExtendedParsedDatum-hostAddress", "ExtendedParsedDatum-parsedText", "ExtendedParsedDatum-language", "ExtendedParsedDatum-title", "ExtendedParsedDatum-outLinks", "ExtendedParsedDatum-parsedMeta","UrlDatum-url", "PayloadDatum-payload");
        Pipe scoredLinksPipe = new CoGroup(SCORED_LINKS_PIPE_NAME, 
        		classifierPipe, new Fields(UrlDatum.URL_FN),
        		parsedPipe, new Fields(UrlDatum.URL_FN),f, new RightJoin());
     
       	scoredLinksPipe = new Each(scoredLinksPipe, new ScoreLinks(classifier));        
        setTails(classifierPipe,scoredLinksPipe);
    }

    public Pipe getTailPipe() {
        String[] pipeNames = getTailNames();
        if (pipeNames.length != 1) {
            throw new RuntimeException("Unexpected number of tail pipes!");
        }
        
        if (!pipeNames[0].equals(CLASSIFIER_PIPE_NAME)) {
            throw new RuntimeException("Unexpected name for tail pipe");
        }
        
        return getTails()[0];
    }
    public Pipe getClassifierTailPipe() {
    	return getTailPipe(CLASSIFIER_PIPE_NAME);    	
    }
    public Pipe getScoredLinksTailPipe() {
    	return getTailPipe(SCORED_LINKS_PIPE_NAME);    	
    }
    public Pipe getTailPipe(String pipeName){
    	if (pipeName.equals(CLASSIFIER_PIPE_NAME))
    		return getTails()[0];
    	else
    		return getTails()[1];
    }
}
