package gr.ilsp.fmc.operations;

import gr.ilsp.fmc.datums.ExtendedUrlDatum;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.bixolabs.cascading.NullContext;

public class SelectUrlOnlyFunction extends BaseOperation<NullContext> implements Function<NullContext> {

	private static final long serialVersionUID = -3658090283230915440L;
	public SelectUrlOnlyFunction() {  
		super(new Fields(ExtendedUrlDatum.URL_FN));
	}
	@Override
	public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
		String url = funcCall.getArguments().get(ExtendedUrlDatum.URL_FN).toString();
		Tuple tup = new Tuple(url);
		funcCall.getOutputCollector().add(tup);			
	}
}