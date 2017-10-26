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
package gr.ilsp.fmc.datums;

import bixo.datum.UrlDatum;


import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class ExtendedUrlDatum extends UrlDatum {
    
	public static final String SCORE_FN = fieldName(ExtendedUrlDatum.class, "score");
    public static final Fields FIELDS = new Fields(SCORE_FN).append(getSuperFields(ExtendedUrlDatum.class));
    
    
    public ExtendedUrlDatum() {
        super(FIELDS);
    }
    
    public ExtendedUrlDatum(ExtendedUrlDatum datum) {
        super(new TupleEntry(datum.getTupleEntry()));
    }
    
    public ExtendedUrlDatum(Fields fields) {
        super(fields);
        validateFields(fields, FIELDS);
    }

    public ExtendedUrlDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
        validateFields(fields, FIELDS);
    }

    public ExtendedUrlDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry.getFields(), FIELDS);
    }
    
    public ExtendedUrlDatum(Fields fields, Double score) {
        super(fields);
        validateFields(fields, FIELDS);
        setScore(score);
    }
    
    public ExtendedUrlDatum(Double score) {
        super(FIELDS);
        setScore(score);
    }
    public ExtendedUrlDatum(String url) {
        super(FIELDS,url);        
    }

    public Double getScore() {
        return _tupleEntry.getDouble(SCORE_FN);
    }

    public void setScore(Double score) {
        _tupleEntry.set(SCORE_FN, score);
    }
}
