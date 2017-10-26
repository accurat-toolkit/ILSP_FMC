package gr.ilsp.fmc.parser;

import gr.ilsp.fmc.datums.ExtendedParsedDatum;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


import org.apache.log4j.Logger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.apache.tika.utils.CharsetUtils;


import bixo.config.ParserPolicy;
import bixo.datum.FetchedDatum;
import bixo.fetcher.HttpHeaderNames;
import bixo.parser.BaseContentExtractor;
import bixo.parser.SimpleContentExtractor;
import bixo.utils.HttpUtils;
import bixo.utils.IoUtils;


public class SimpleNoLinksParser implements Serializable, Callable<ExtendedParsedDatum> {
    /**
	 * 
	 */
	private static final long serialVersionUID = -6644451796914124629L;

	private static final Logger LOGGER = Logger.getLogger(SimpleNoLinksParser.class);

    private boolean _extractLanguage = true;
    protected BaseContentExtractor _contentExtractor;
    private transient Parser _parser;
    private ParserPolicy _policy;
    private FetchedDatum _datum;

	private boolean _keepBoiler = false;
    
    public SimpleNoLinksParser(FetchedDatum datum) {
    	this();
    	_datum = datum;    	
    }
    public SimpleNoLinksParser() {
        this(new ParserPolicy());
    }
    public SimpleNoLinksParser(boolean keepBoiler) {    	
        this(new ParserPolicy());
        _keepBoiler  = keepBoiler;
    }
    public SimpleNoLinksParser(ParserPolicy parserPolicy) {
        this(new SimpleContentExtractor(),  parserPolicy);
    }
    
    /**
     * @param contentExtractor to use instead of new {@link SimpleContentExtractor}()
     * @param linkExtractor to use instead of new {@link SimpleLinkExtractor}()
     * @param parserPolicy to customize operation of the parser
     * <BR><BR><B>Note:</B> There is no need to construct your own
     * {@link SimpleLinkExtractor} simply to control the set of link tags
     * and attributes it processes. Instead, use {@link ParserPolicy#setLinkTags}
     * and {@link ParserPolicy#setLinkAttributeTypes}, and then pass this policy
     * to {@link SimpleParser#SimpleParser(ParserPolicy)}.
     */
    public SimpleNoLinksParser(BaseContentExtractor contentExtractor, ParserPolicy parserPolicy) {
        _policy = parserPolicy;
        
        _contentExtractor = contentExtractor;        
    }
    
    protected synchronized void init() {
        if (_parser == null) {
            _parser = getTikaParser();
        }
        
        _contentExtractor.reset();        
    }

    
    public Parser getTikaParser() {
        return new AutoDetectParser();
    }

    public void setExtractLanguage(boolean extractLanguage) {
        _extractLanguage = extractLanguage;
    }
    
    public boolean isExtractLanguage() {
        return _extractLanguage;
    }
    
    
    public ExtendedParsedDatum parse(FetchedDatum fetchedDatum) throws Exception {
        init();

        if (LOGGER.isTraceEnabled()) {
        	LOGGER.trace(String.format("Parsing %s", fetchedDatum.getUrl()));
        }
        
        // Provide clues to the parser about the format of the content.
        Metadata metadata = new Metadata();
        metadata.add(Metadata.RESOURCE_NAME_KEY, fetchedDatum.getBaseUrl());
        metadata.add(Metadata.CONTENT_TYPE, fetchedDatum.getContentType());
        String charset = getCharset(fetchedDatum);
        metadata.add(Metadata.CONTENT_LANGUAGE, getLanguage(fetchedDatum, charset));
        metadata.add(Metadata.CONTENT_ENCODING, charset);
        
        
        InputStream is = new ByteArrayInputStream(fetchedDatum.getContentBytes(), 0, fetchedDatum.getContentLength());
        metadata.add(Metadata.CONTENT_LENGTH, Integer.toString(fetchedDatum.getContentLength()));
        try {
        	URL baseUrl = getContentLocation(fetchedDatum);
        	metadata.add(Metadata.CONTENT_LOCATION, baseUrl.toExternalForm());

            Callable<ExtendedParsedDatum> c = new TikaCallableParser(_parser, _contentExtractor,  is, metadata, isExtractLanguage(), _keepBoiler);
            FutureTask<ExtendedParsedDatum> task = new FutureTask<ExtendedParsedDatum>(c);
            Thread t = new Thread(task);
            t.start();
            
            ExtendedParsedDatum result;
            try {
                result = task.get(getParserPolicy().getMaxParseDuration(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                task.cancel(true);
                t.interrupt();
                throw e;
            } finally {
                t = null;
            }
            //result.setParsedText(ArticleExtractor.INSTANCE.getText(new InputStreamReader(is)));
            // TODO KKr Should there be a BaseParser to take care of copying
            // these two fields?
            result.setHostAddress(fetchedDatum.getHostAddress());
            result.setPayload(fetchedDatum.getPayload());
            return result;
        } finally {
            IoUtils.safeClose(is);
        }
    }

    protected URL getContentLocation(FetchedDatum fetchedDatum) throws MalformedURLException {
		URL baseUrl = new URL(fetchedDatum.getFetchedUrl());
		
		// See if we have a content location from the HTTP headers that we should use as
		// the base for resolving relative URLs in the document.
		String clUrl = fetchedDatum.getHeaders().getFirst(HttpHeaderNames.CONTENT_LOCATION);
		if (clUrl != null) {
			// FUTURE KKr - should we try to keep processing if this step fails, but
			// refuse to resolve relative links?
			baseUrl = new URL(baseUrl, clUrl);
		}
		return baseUrl;
	}

    /**
     * Extract encoding from content-type
     * 
     * If a charset is returned, then it's a valid/normalized charset name that's
     * supported on this platform.
     * 
     * @param datum
     * @return charset in response headers, or null
     */
    protected String getCharset(FetchedDatum datum) {
        return CharsetUtils.clean(HttpUtils.getCharsetFromContentType(datum.getContentType()));
    }

    /**
     * Extract language from (first) explicit header
     * 
     * @param fetchedDatum
     * @param charset 
     * @return first language in response headers, or null
     */
    protected String getLanguage(FetchedDatum fetchedDatum, String charset) {
        return fetchedDatum.getHeaders().getFirst(HttpHeaderNames.CONTENT_LANGUAGE);
    }
    

    public ParserPolicy getParserPolicy() {
        return _policy;
    }
	
	@Override
	public ExtendedParsedDatum call() throws Exception {
		// TODO Auto-generated method stub
		return parse(_datum);
	}
}
