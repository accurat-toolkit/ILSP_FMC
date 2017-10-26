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
package gr.ilsp.fmc.mysql;

import gr.ilsp.fmc.datums.ClassifierDatum;
import gr.ilsp.fmc.datums.CrawlDbDatum;
import gr.ilsp.fmc.datums.ExtendedParsedDatum;
import gr.ilsp.fmc.datums.StatusOutputDatum;
import gr.ilsp.fmc.main.SimpleCrawl;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.hsqldb.Server;

import bixo.datum.FetchedDatum;

import bixo.examples.JDBCTapFactory;

import com.bixolabs.cascading.HadoopUtils;

import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
import cascading.jdbc.TableDesc;
import cascading.tap.Tap;
import cascading.tuple.Fields;
@SuppressWarnings("deprecation")
public class MYSQLTapFactory {

    private static final Logger LOGGER = Logger.getLogger(JDBCTapFactory.class);
    //private static final String JDBC_URL_PREFIX = "jdbc:hsqldb:";
    //private static final String DB_NAME = "sitecrawler";
    //private static final String IN_MEM_DB = "mem:" + DB_NAME;
    //private static final String PERSISTENT_DB_PREFIX = "file:";
    
    //private static final String JDBC_SERVER_URL_PREFIX = JDBC_URL_PREFIX + "hsql://";
    //private static final String JDBC_SERVER_SUFFIX = "/" + DB_NAME + ";shutdown=true";
        
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_DB_PREFIX = "jdbc:mysql://";
    
    private static final String[] _urlsSinkColumnNames = {"url", "lastFetched", "lastUpdated", "lastStatus", "crawlDepth","score"};
    private static final String[] _urlsSinkColumnDefs = {"TEXT BINARY", "BIGINT", "BIGINT", "VARCHAR(32)", "INTEGER", "DOUBLE"};

    private static String _jdbcUrl;
    private static Server _server;
    
    public static Tap createUrlsSourceJDBCTap(String dbLocation, String dbname) {
        String[] primaryKeys = {};
        
        initRunEnvironment(dbLocation, dbname);
        
        String driver = MYSQL_DRIVER;
        String tableName = "urls";
        //String[] orderBy = new String[] {"url"};
        //String URL = com.bixolabs.cascading.BaseDatum.fieldName(CrawlDbDatum.class,"url");
    	//Fields updateByFields = new Fields(URL);
		//String[] updateBy = new String[] {"url"};
        //String selectQuery = "SELECT * FROM urls WHERE lastStatus = 'UNFETCHED' AND url NOT IN " +
        //		"(SELECT url FROM urls WHERE lastSTATUS='FETCHED') group by url";
        //String countQuery = "SELECT COUNT(*) FROM urls WHERE lastFetched = 0 AND url NOT IN " +
        //		"(SELECT url FROM urls WHERE lastFetched>0) group by url";
        TableDesc tableDesc = new TableDesc( tableName, _urlsSinkColumnNames, _urlsSinkColumnDefs, primaryKeys );
        String[] orderBy = new String[] {"score DESC"};
        Tap urlsTap = new JDBCTap( _jdbcUrl, driver, tableDesc, new JDBCScheme( CrawlDbDatum.FIELDS, _urlsSinkColumnNames, orderBy));                                         
        return urlsTap;

    }
    public static Tap createUrlUpdateSink(String dbLocation, String dbname){
    	String[] primaryKeys = {};
    	initRunEnvironment(dbLocation, dbname);
        String driver = MYSQL_DRIVER;
        String tableName = "urls";
        String[] orderBy = new String[] {"url"};
        String URL = com.bixolabs.cascading.BaseDatum.fieldName(CrawlDbDatum.class,"url");
    	Fields updateByFields = new Fields(URL);
		String[] updateBy = new String[] {"url"};
		TableDesc tableDesc = new TableDesc( tableName, _urlsSinkColumnNames, _urlsSinkColumnDefs, primaryKeys );
        Tap urlsTap = new JDBCTap( _jdbcUrl, driver, tableDesc, new JDBCScheme( CrawlDbDatum.FIELDS, _urlsSinkColumnNames,orderBy,updateByFields,updateBy));                                         
        return urlsTap;
    }
    /*public static Tap createUrlsSourceJDBCTap(String dbLocation) {
        String[] primaryKeys = {"url"};
        
        initRunEnvironment(dbLocation);
        return createUrlsTap(primaryKeys, dbLocation);
    }*/
    
    // Similar to Urls Source Tap except that it doesn't have a primary key - by doing this
    // we 'fool' JDBCTap into thinking that the source and sink url taps aren't the same.
    public static Tap createUrlsSinkJDBCTap(String dbLocation, String dbname) {
        String[] primaryKeys = {};
        return createUrlsTap(primaryKeys, dbLocation, dbname);
    }

    //public static Tap createUrlsSink2JDBCTap(String dbLocation) {
    //    String[] primaryKeys = {"lastFetched"};
    //    return createUrlsTap(primaryKeys, dbLocation);
    //}

    private static Tap createUrlsTap(String[] primaryKeys, String dbLocation, String dbname) {
        initRunEnvironment(dbLocation, dbname);
        
        String driver = MYSQL_DRIVER;
        String tableName = "urls";
        //String[] orderBy = new String[] {"url"};
        //String URL = com.bixolabs.cascading.BaseDatum.fieldName(CrawlDbDatum.class,"url");
    	//Fields updateByFields = new Fields(URL);
		//String[] updateBy = new String[] {"url"};
        TableDesc tableDesc = new TableDesc( tableName, _urlsSinkColumnNames, _urlsSinkColumnDefs, primaryKeys);
        Tap urlsTap = new JDBCTap( _jdbcUrl, driver, tableDesc, new JDBCScheme( CrawlDbDatum.FIELDS, _urlsSinkColumnNames));        
        
        return urlsTap;
  
    }
    
    public static Tap createContentSinkJDBCTap(String dbLocation, String dbname){
    	initRunEnvironment(dbLocation,dbname);
    	String[] primaryKeys = {};
    	String driver = MYSQL_DRIVER;
        String tableName = "content";
        String[] contentSinkColumnNames = { "newBaseUrl" , "fetchedUrl", "fetchTime", "content", "contentType", "responseRate"
        		, "numRedirects", "hostAddress", "httpHeaders", "url", "payload"};
        String[] contentSinkColumnDefs = {"TEXT", "TEXT", "BIGINT", "MEDIUMBLOB", "VARCHAR(127)","BIGINT","INTEGER",
        		"VARCHAR(127)","MEDIUMBLOB", "TEXT", "MEDIUMBLOB"};
        TableDesc tableDesc = new TableDesc( tableName, contentSinkColumnNames, contentSinkColumnDefs, primaryKeys );        
        
		Tap contentTap = new JDBCTap( _jdbcUrl, driver, tableDesc, new JDBCScheme( FetchedDatum.FIELDS, contentSinkColumnNames));
    	return contentTap;
    }
    
    public static Tap createParseSinkJDBCTap(String dbLocation, String dbname){
    	initRunEnvironment(dbLocation,dbname);
    	String[] primaryKeys = {};
    	String driver = MYSQL_DRIVER;
        String tableName = "parse";
        String[] parseSinkColumnNames = { "hostAddress" , "parsedText", "language", "title", "outLinks", "parsedMeta"
        		 ,"url", "payload"};
        String[] parseSinkColumnDefs = {"VARCHAR(127)", "MEDIUMBLOB", "VARCHAR(127)", "TEXT", "MEDIUMBLOB","MEDIUMBLOB","TEXT",
        		"MEDIUMBLOB"};
        TableDesc tableDesc = new TableDesc( tableName, parseSinkColumnNames, parseSinkColumnDefs, primaryKeys );        
        Tap contentTap = new JDBCTap( _jdbcUrl, driver, tableDesc, new JDBCScheme( ExtendedParsedDatum.FIELDS, parseSinkColumnNames));
    	return contentTap;
    }
    
    public static Tap createClassifierSinkJDBCTap(String dbLocation, String dbname){
    	initRunEnvironment(dbLocation,dbname);
    	String[] primaryKeys = {};
    	String driver = MYSQL_DRIVER;
        String tableName = "classified";
        String[] classifierSinkColumnNames = { "subclasses" , "subscores", "totabscore", "totrelscore", "url", "payload"};
        String[] classifierSinkColumnDefs = {"MEDIUMBLOB", "MEDIUMBLOB", "DOUBLE", "DOUBLE", "TEXT",	"MEDIUMBLOB"};
        TableDesc tableDesc = new TableDesc( tableName, classifierSinkColumnNames, classifierSinkColumnDefs, primaryKeys );        
        Tap contentTap = new JDBCTap( _jdbcUrl, driver, tableDesc, new JDBCScheme( ClassifierDatum.FIELDS, classifierSinkColumnNames));
    	return contentTap;
    }
    public static Tap createStatusSinkJDBCTap(String dbLocation, String dbname){
    	initRunEnvironment(dbLocation,dbname);
    	String[] primaryKeys = {};
    	String driver = MYSQL_DRIVER;
        String tableName = "status";
        String[] statusSinkColumnNames = { "status", "headers", "exception", "statusTime", "hostAddress", "url", "payload" };
        String[] statusSinkColumnDefs = {"VARCHAR(127)", "BLOB", "BLOB", "BIGINT", "VARCHAR(127)", "TEXT", "MEDIUMBLOB"};
        TableDesc tableDesc = new TableDesc( tableName, statusSinkColumnNames, statusSinkColumnDefs, primaryKeys );        
        Tap contentTap = new JDBCTap( _jdbcUrl, driver, tableDesc, new JDBCScheme( StatusOutputDatum.FIELDS, statusSinkColumnNames));
    	return contentTap;
    }
    
    private static void initRunEnvironment(String dbLocation, String dbname) {
        if (_jdbcUrl == null) {
            JobConf jobConf;
            try {
                jobConf = HadoopUtils.getDefaultJobConf();
            } catch (IOException e) {
                throw new RuntimeException("Unable to get default job conf: " + e);
            }
            //String db = IN_MEM_DB;
            //if (dbLocation != null) {
            //   String separator = "";
            //    if (!dbLocation.endsWith("/")) {
            //        separator = "/";
            //    }
            //    db = PERSISTENT_DB_PREFIX + dbLocation + separator + DB_NAME;
            //}
            
            if (HadoopUtils.isJobLocal(jobConf)) {
                //_jdbcUrl = JDBC_URL_PREFIX + db;
                _jdbcUrl = MYSQL_DB_PREFIX+dbLocation+"/" + dbname + "?characterSetResults=UTF-8&characterEncoding=UTF-8" +
                		"&useUnicode=yes&user=" + SimpleCrawl.config.getString("mysql_server.user") + 
                		"&password=" + SimpleCrawl.config.getString("mysql_server.password");
            } else {
            	_jdbcUrl = MYSQL_DB_PREFIX+dbLocation+"/" + dbname + "?characterSetResults=UTF-8&characterEncoding=UTF-8&useUnicode=yes&user="
            			+ SimpleCrawl.config.getString("mysql_server.user") + 
                		"&password=" + SimpleCrawl.config.getString("mysql_server.password");
                /*if (_server == null) {
                    try {
                        InetAddress addr = InetAddress.getLocalHost();
                        String hostAddress = addr.getHostAddress();
                        _jdbcUrl = JDBC_SERVER_URL_PREFIX + hostAddress + JDBC_SERVER_SUFFIX;
                    } catch (UnknownHostException e) {
                        throw new RuntimeException("Unable to get host address: " + e);
                    }
                    String serverProps = "database.0=" + db;
                    _server = new Server();
                    _server.putPropertiesFromString(serverProps);
                    _server.setDatabaseName(0, "sitecrawler");
                    _server.setLogWriter(null);
                    _server.setErrWriter(null);
                    _server.start();
                }*/
                LOGGER.info("Using MYSQL in server mode");
            }
        }
    }

    public static void shutdown() {
        if (_server != null) {
            _server.shutdown();
        }
    }
}
