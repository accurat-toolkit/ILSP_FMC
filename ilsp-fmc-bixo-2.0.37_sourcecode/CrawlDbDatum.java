package gr.ilsp.fmc.datums;


import bixo.datum.UrlStatus;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.bixolabs.cascading.BaseDatum;

@SuppressWarnings("serial")
public class CrawlDbDatum extends BaseDatum {
    

    public static final String URL_FIELD = fieldName(CrawlDbDatum.class, "url");
    public static final String LAST_FETCHED_FIELD = fieldName(CrawlDbDatum.class, "lastFetched");
    public static final String LAST_UPDATED_FIELD = fieldName(CrawlDbDatum.class, "lastUpdated");
    public static final String LAST_STATUS_FIELD = fieldName(CrawlDbDatum.class, "lastStatus");
    public static final String CRAWL_DEPTH = fieldName(CrawlDbDatum.class, "crawlDepth");
    public static final String SCORE = fieldName(CrawlDbDatum.class, "score");
    
    
    public static final Fields FIELDS = new Fields(URL_FIELD, LAST_FETCHED_FIELD, LAST_UPDATED_FIELD, LAST_STATUS_FIELD, CRAWL_DEPTH, SCORE);

    public CrawlDbDatum () {
        super(FIELDS);
    }

    public CrawlDbDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry.getFields(), FIELDS);
    }
    
    public CrawlDbDatum(String url, long lastFetched, long lastUpdated, UrlStatus lastStatus, int crawlDepth, double score) {
        super(FIELDS);
        setUrl(url);
        setLastFetched(lastFetched);
        setLastUpdated(lastUpdated);
        setLastStatus(lastStatus);
        setCrawlDepth(crawlDepth);
        setScore(score);
       }

    public String getUrl() {
        return _tupleEntry.getString(URL_FIELD);
    }

    public void setUrl(String url) {
        _tupleEntry.set(URL_FIELD, url);
    }

    public long getLastFetched() {
        return _tupleEntry.getLong(LAST_FETCHED_FIELD);
    }

    public void setLastFetched(long lastFetched) {
        _tupleEntry.set(LAST_FETCHED_FIELD, lastFetched);
    }

    public long getLastUpdated() {
        return _tupleEntry.getLong(LAST_UPDATED_FIELD);
    }

    public void setLastUpdated(long lastUpdated) {
        _tupleEntry.set(LAST_UPDATED_FIELD, lastUpdated);
    }

    public UrlStatus getLastStatus() {
        return UrlStatus.valueOf(_tupleEntry.getString(LAST_STATUS_FIELD));
    }

    public void setLastStatus(UrlStatus lastStatus) {
        _tupleEntry.set(LAST_STATUS_FIELD, lastStatus.name());
    }

    public int getCrawlDepth() {
        return _tupleEntry.getInteger(CRAWL_DEPTH);
    }

    public void setCrawlDepth(int crawlDepth) {
        _tupleEntry.set(CRAWL_DEPTH, crawlDepth);
    }
    public void setScore(double score) {
        _tupleEntry.set(SCORE, score);
    }
    public double getScore() {
        return _tupleEntry.getDouble(SCORE);
    }

    public String toString() {
        return getUrl() + "\t" + getLastFetched() + "\t" + getLastUpdated() + "\t" + getLastStatus() + "\t" + getCrawlDepth() + "\t" + getScore();
    }
}
