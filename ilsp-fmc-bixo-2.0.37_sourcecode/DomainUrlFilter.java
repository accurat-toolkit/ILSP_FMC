package gr.ilsp.fmc.parser;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import bixo.datum.UrlDatum;
import bixo.urls.BaseUrlFilter;

// Filter URLs that fall outside of the target domain
@SuppressWarnings({ "serial", "deprecation" })
public class DomainUrlFilter extends BaseUrlFilter {
	private static final Logger LOGGER = Logger.getLogger(DomainUrlFilter.class);

	private ArrayList<String> _domain = new ArrayList<String>();
	private Pattern _suffixExclusionPattern;
	private Pattern _protocolInclusionPattern;

	//public DomainUrlFilter() {
	//	this(null);
	//}
	public DomainUrlFilter(String domain) {
		if (domain!=null)
			_domain.add(domain);
		_suffixExclusionPattern = Pattern.compile("(?i)\\.(pdf|zip|gzip|gz|sit|bz|bz2|tar|tgz|exe|arff|au" +
				"|avi|class|fig|gif|hqx|ica|jpeg|jpg|mat|mdb|mov|mp3|mpeg|mpg|msi|pcx|pdb|psd|ram|rar|raw|rmd|rmx|" +
				"sav|sdd|shar|tga|tif|tiff|vo|wav|wmv|wmz|xbm|xpm|z)$");
		_protocolInclusionPattern = Pattern.compile("(?i)^(http|https)://");
	}
	public DomainUrlFilter(Path domain){
		JobConf conf = new JobConf();
		_suffixExclusionPattern = Pattern.compile("(?i)\\.(pdf|zip|gzip|gz|sit|bz|bz2|tar|tgz|exe|arff|au" +
				"|avi|class|fig|gif|hqx|ica|jpeg|jpg|mat|mdb|mov|mp3|mpeg|mpg|msi|pcx|pdb|psd|ram|rar|raw|rmd|rmx|" +
				"sav|sdd|shar|tga|tif|tiff|vo|wav|wmv|wmz|xbm|xpm|z)$");
		_protocolInclusionPattern = Pattern.compile("(?i)^(http|https)://");
		try {
			FileSystem fs = domain.getFileSystem(conf);		
			BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(domain),"UTF8"));
			String line = "";
			while ((line=rdr.readLine())!=null){
				if (!line.isEmpty()) {
					if (line.contains("/")) line = line.substring(0,line.length()-1);
					_domain.add(line);
				
				}
			}
			rdr.close();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean isRemove(UrlDatum datum) {
		String urlAsString = datum.getUrl();

		// Skip URLs with protocols we don't want to try to process
		if (!_protocolInclusionPattern.matcher(urlAsString).find()) {
			return true;

		}

		if (_suffixExclusionPattern.matcher(urlAsString).find()) {
			return true;
		}

		try {
			URL url = new URL(urlAsString);
			if (_domain.size()!=0){
				String host = url.getHost();				
				//return (!host.endsWith(_domain));
				for (String s:_domain){
					if (s!=null)
						if (host.endsWith(s)) return false;					
				}
				return true;
			} else
				return false;
		} catch (MalformedURLException e) {
			LOGGER.warn("Invalid URL: " + urlAsString);
			return true;
		}
	}
}
