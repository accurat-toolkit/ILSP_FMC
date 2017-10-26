package gr.ilsp.fmc.datums;

import bixo.datum.StatusDatum;
import bixo.datum.UrlDatum;
import bixo.datum.UrlStatus;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.bixolabs.cascading.Payload;

@SuppressWarnings("serial")
public class StatusOutputDatum extends UrlDatum {
    
    public static final String STATUS_FN = fieldName(StatusOutputDatum.class, "status");
    public static final String HEADERS_FN = fieldName(StatusOutputDatum.class, "headers");
    public static final String EXCEPTION_FN = fieldName(StatusOutputDatum.class, "exception");
    public static final String STATUS_TIME_FN = fieldName(StatusOutputDatum.class, "statusTime");
    public static final String HOST_ADDRESS_FN = fieldName(StatusOutputDatum.class, "hostAddress");
        
    public static final Fields FIELDS = new Fields(STATUS_FN, HEADERS_FN, EXCEPTION_FN, STATUS_TIME_FN, HOST_ADDRESS_FN).append(getSuperFields(StatusDatum.class));

    public StatusOutputDatum() {
        super(FIELDS);
    }
    
    public StatusOutputDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }
    
    /**
     * Constructor for creating StatusDatum for a URL that was fetched successfully.
     * 
     * @param url URL that we fetched.
     * @param headers Headers returned by server.
     * @param hostAddress Host IP address of server.
     * @param payload User-provided payload.
     */
    public StatusOutputDatum(String url, String headers, String hostAddress, Payload payload) {
        this(url, UrlStatus.FETCHED, headers, null, System.currentTimeMillis(), hostAddress, payload);
    }
    
    //public StatusOutputDatum(String url, BaseFetchException e, Payload payload) {
    //    this(url, e.mapToUrlStatus(), null, e, System.currentTimeMillis(), null, payload);
    //}
    
    public StatusOutputDatum(String url, UrlStatus status, Payload payload) {
        this(url, status, null, null, System.currentTimeMillis(), null, payload);
    }
    
    public StatusOutputDatum(String url, UrlStatus status, String headers, String exception, long statusTime, String hostAddress, Payload payload) {
        super(FIELDS);
        
        setUrl(url);
        setStatus(status);
        setHeaders(headers);
        setException(exception);
        setStatusTime(statusTime);
        setHostAddress(hostAddress);
        setPayload(payload);
    }

    public UrlStatus getStatus() {
        return UrlStatus.valueOf(_tupleEntry.getString(STATUS_FN));
    }

    public void setStatus(UrlStatus status) {
        _tupleEntry.set(STATUS_FN, status.name());
    }
    
    //public HttpHeaders getHeaders() {
    //    return new HttpHeaders((Tuple)_tupleEntry.get(HEADERS_FN));
    //}
    public String getHeaders() {
        return _tupleEntry.getString(HEADERS_FN);
    }
    public void setHeaders(String headers) {
    	_tupleEntry.set(HEADERS_FN,headers);
    }
    //public void setHeaders(HttpHeaders headers) {
    //    if (headers == null) {
    //        _tupleEntry.set(HEADERS_FN, null);
    //    } else {
    //        _tupleEntry.set(HEADERS_FN, headers.toTuple());
    //    }
    //}
    
    public String getException() {
        return _tupleEntry.getString(EXCEPTION_FN);
    }

    public void setException(String e) {
        _tupleEntry.set(EXCEPTION_FN, e);
    }
    
    public long getStatusTime() {
        return _tupleEntry.getLong(STATUS_TIME_FN);
    }
    
    public void setStatusTime(long statusTime) {
        _tupleEntry.set(STATUS_TIME_FN, statusTime);
    }
    
    public String getHostAddress() {
        return _tupleEntry.getString(HOST_ADDRESS_FN);
    }
    
    public void setHostAddress(String hostAddress) {
        _tupleEntry.set(HOST_ADDRESS_FN, hostAddress);
    }

    public static Fields getGroupingField() {
        return new Fields(UrlDatum.URL_FN);
    }

}
