/**
 * 
 */
package gr.ilsp.fmc.parser;


import gr.ilsp.fmc.datums.ExtendedParsedDatum;
import gr.ilsp.fmc.utils.ContentNormalizer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.tika.language.ProfilingHandler;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.DefaultHtmlMapper;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.sax.TeeContentHandler;

import de.l3s.boilerpipe.extractors.NumWordsRulesExtractor;





import bixo.parser.BaseContentExtractor;

public class TikaCallableParser implements Callable<ExtendedParsedDatum> {
    //private static final Logger LOGGER = Logger.getLogger(TikaCallableParser.class);
    
    

    // Simplistic language code pattern used when there are more than one languages specified
    // FUTURE KKr - improve this to handle en-US, and "eng" for those using old-style language codes.
    //private static final Pattern LANGUAGE_CODE_PATTERN = Pattern.compile("([a-z]{2})([,;-]).*");

    private Parser _parser;
    private BaseContentExtractor _contentExtractor;
    private InputStream _input;
    private Metadata _metadata;
    private boolean _extractLanguage;
	private boolean _keepBoiler = false;
    
    public TikaCallableParser(Parser parser, BaseContentExtractor contentExtractor, InputStream input, Metadata metadata) {
        this(parser, contentExtractor, input, metadata, true, false);
    }
    
    public TikaCallableParser(Parser parser, BaseContentExtractor contentExtractor, InputStream input, Metadata metadata, boolean extractLanguage, boolean keepBoiler) {
        _parser = parser;
        _contentExtractor = contentExtractor;
        _input = input;
        _metadata = metadata;
        _extractLanguage = extractLanguage;
        _keepBoiler  = keepBoiler;
    }
    
    @Override
    public ExtendedParsedDatum call() throws Exception {
        try {        	 	        	
            TeeContentHandler teeContentHandler;
            ProfilingHandler profilingHandler = null;
            
            if (_extractLanguage) {
                profilingHandler = new ProfilingHandler();
                teeContentHandler = new TeeContentHandler(_contentExtractor,  profilingHandler);
            } else {
                teeContentHandler = new TeeContentHandler(_contentExtractor);
            }
            //ISSUE with Content-Encoding reported error by meta in content while it is correct
            //from the HTTp-response. This is official TIKA bug (https://issues.apache.org/jira/browse/TIKA-539)
            //Temp solution until bug is resolved: keep the HTTP response META (if exists) and overwrite
            String respoCharset = _metadata.get(Metadata.CONTENT_ENCODING);
            _parser.parse(_input, teeContentHandler, _metadata, makeParseContext());
            if (respoCharset!=null && respoCharset!=_metadata.get(Metadata.CONTENT_ENCODING))
            	_metadata.set(Metadata.CONTENT_ENCODING, respoCharset);
            
            
            ExtendedOutlink[] outlinks = ExtendedLinksExtractor.getLinks(_input,_metadata);
            //String lang = _extractLanguage ? detectLanguage(_metadata, profilingHandler) : "";
            String lang = "";
            _input.reset();
            
            
            /*Callable<ExtendedOutlink[]> linkcallable = new CallableLinkExtractor(_input,_metadata);
            Callable<String> c = new CallableBoilerplateRemover(_input,_metadata.get(Metadata.CONTENT_ENCODING));
            ExecutorService taskExecutor = Executors.newFixedThreadPool(2);
            Future<ExtendedOutlink[]> linktask =taskExecutor.submit(linkcallable);
            Future<String> task = taskExecutor.submit(c);
            taskExecutor.shutdown();
            
            ExtendedOutlink[] outlinks;
            String content;
            taskExecutor.awaitTermination(28000, TimeUnit.MILLISECONDS);
            content = task.get();
            outlinks = linktask.get();*/
            
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(_input,_metadata.get(Metadata.CONTENT_ENCODING)));            
            String content = "";            
            /*if (_keepBoiler) {            	
            	content = gr.ilsp.boilerpipe.extractors.NumWordsRulesExtractor.INSTANCE.getText(reader,true);
            } else {
            	content = NumWordsRulesExtractor.INSTANCE.getText(reader);
            }*/
            content = gr.ilsp.boilerpipe.extractors.NumWordsRulesExtractor.INSTANCE.getText(reader,true);
            
            
            
            //Remove all consecutive occasions of whitespace characters 
            //content = content.replaceAll("(\\s|\\xA0){2,}", " ");
            content = ContentNormalizer.normalizeText(content);
            
            return new ExtendedParsedDatum(_metadata.get(Metadata.RESOURCE_NAME_KEY), null, /*_contentExtractor.getContent()*/content, lang,
                            _metadata.get(Metadata.TITLE), outlinks,makeMap(_metadata));
        } catch (Exception e) {
            // Generic exception that's OK to re-throw
            throw e;
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("Attempting to use excluded parser");
        } catch (Throwable t) {
            // Make sure nothing inside Tika can kill us
            throw new RuntimeException("Serious shut-down error thrown from Tika", t);
        }
    }
    
    
   

	/**
     * Decide if we need to set up our own HtmlMapper, because the link extractor has tags that
     * aren't part of the default set.
     * 
     * @return
     */
    private ParseContext makeParseContext() {
        ParseContext result = new ParseContext();       
        HtmlMapper defaultMapper = DefaultHtmlMapper.INSTANCE;        
        result.set(HtmlMapper.class, defaultMapper);
        return result;
    }

    /**
     * See if a language was set by the parser, from meta tags.
     * As a last resort falls back to the result from the ProfilingHandler.
     *  
     * @param metadata
     * @param profilingHandler
     * @return The first language found (two char lang code) or empty string if no language was detected.
     */
    /*private static String detectLanguage(Metadata metadata, ProfilingHandler profilingHandler) {
        String result = null;
        
        String dubCoreLang = metadata.get(Metadata.LANGUAGE);
        String httpEquivLang = metadata.get(Metadata.CONTENT_LANGUAGE);
        
        if (dubCoreLang != null) {
            result = dubCoreLang;
        } else if (httpEquivLang != null) {
            result = httpEquivLang;
        }
        
        result = getFirstLanguage(result);
        
        if (result == null) {
            // Language is still unspecified, so use ProfileHandler's result
            LanguageIdentifier langIdentifier = profilingHandler.getLanguage();
            // FUTURE KKr - provide config for specifying required certainty level.
            if (langIdentifier.isReasonablyCertain()) {
                result = langIdentifier.getLanguage();
                LOGGER.trace("Using language specified by profiling handler: " + result);
            } else {
                result = "";
            }

        }
        
        return result;
    }*/

    private static Map<String, String> makeMap(Metadata metadata) {
        Map<String, String> result = new HashMap<String, String>();
        
        for (String key : metadata.names()) {
            result.put(key, metadata.get(key));
        }
        
        return result;
    }


    /*private static String getFirstLanguage(String lang) {
        if (lang != null && lang.length() > 0) {
            // TODO VMa -- DublinCore languages could be specified in a multiple of ways
            // see : http://dublincore.org/documents/2000/07/16/usageguide/qualified-html.shtml#language
            // This means that it is possible to get back 3 character language strings as per ISO639-2
            // For now, we handle just two character language strings and if we do get a 3 character string we 
            // treat it as a "null" language.
            
            // TODO VMa - what if the length is just one char ?
            if (lang.length() > 2) {
                Matcher m = LANGUAGE_CODE_PATTERN.matcher(lang);
                
                if (m.matches()) {
                    lang = m.group(1);
                } else {
                    lang = null;
                }
            }
        } 
        return lang;
    }*/

}