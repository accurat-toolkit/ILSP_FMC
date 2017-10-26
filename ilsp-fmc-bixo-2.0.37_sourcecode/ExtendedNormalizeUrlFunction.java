package gr.ilsp.fmc.operations;


import gr.ilsp.fmc.datums.ExtendedUrlDatum;
import bixo.urls.BaseUrlNormalizer;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;

import com.bixolabs.cascading.NullContext;

@SuppressWarnings("serial")
public class ExtendedNormalizeUrlFunction extends BaseOperation<NullContext> implements Function<NullContext> {

    private final BaseUrlNormalizer _normalizer;

    public ExtendedNormalizeUrlFunction(BaseUrlNormalizer normalizer) {
        super(ExtendedUrlDatum.FIELDS);
        
        _normalizer = normalizer;
    }

    @Override
    public void operate(FlowProcess process, FunctionCall<NullContext> funCall) {
        ExtendedUrlDatum datum = new ExtendedUrlDatum(funCall.getArguments());
        
        // Create copy, since we're setting a field, and the tuple is going to be unmodifiable.
        ExtendedUrlDatum result = new ExtendedUrlDatum(datum);
        result.setUrl(_normalizer.normalize(datum.getUrl()));
        funCall.getOutputCollector().add(result.getTuple());
    }
}
