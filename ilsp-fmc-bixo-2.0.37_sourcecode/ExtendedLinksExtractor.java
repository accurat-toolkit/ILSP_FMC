package gr.ilsp.fmc.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


import java.util.List;
import java.util.StringTokenizer;

import org.apache.tika.metadata.Metadata;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;





public class ExtendedLinksExtractor {
	private static final int context_thresh=40;
	public static ExtendedOutlink[] getLinks(InputStream _input, Metadata metadata) {
		//ArrayList<String[]> rankedlinks = new ArrayList<String[]>();
		org.jsoup.nodes.Document doc;
		ExtendedOutlink[] rankedLinks = null;
		try {
			_input.reset();
			BufferedReader reader = new BufferedReader(new InputStreamReader(_input,metadata.get(Metadata.CONTENT_ENCODING)));
			
			String htmltext = "", linktext = "", anchortext = "";
			String line = "", temp = "";
			String pre_extendedtext ="", post_extendedtext ="", wholetext="";
			int countpre=0, countpost=0, count=0;
			int prelimit=0, postlimit=0;
			int index = 0;
			List<org.jsoup.nodes.Node> aa = null;
			org.jsoup.nodes.Element node = null;
			StringTokenizer stpre = null,stpost = null;
			String[] preWORDS=null, postWORDS=null;
			
			while ((line=reader.readLine())!=null) htmltext=htmltext.concat(line);
			reader.close();
			String baseUrl = metadata.get(Metadata.CONTENT_LOCATION);			
			doc = baseUrl!=null ? Jsoup.parse(htmltext,baseUrl) : Jsoup.parse(htmltext);		
			Elements links = doc.select("a[href]");
			rankedLinks = new ExtendedOutlink[links.size()];	
			int linksIndex = 0;
			for (org.jsoup.nodes.Element link : links) {
				linktext = link.attr("abs:href");
				anchortext = link.text().trim();
				aa=link.parent().childNodes();
				index = aa.indexOf(link);
				pre_extendedtext ="";
				post_extendedtext ="";
				wholetext="";
				countpre=0;
				countpost=0;
				count=0;
				for (int bb=1;bb<aa.size();bb++){
					if (index-bb>-1){
						temp = aa.get(index-bb).getClass().toString();
						if (temp.equals("class org.jsoup.nodes.TextNode")){
							pre_extendedtext= aa.get(index-bb)+" "+pre_extendedtext;
						}else if (temp.equals("class org.jsoup.nodes.Element")){
							node = (org.jsoup.nodes.Element) aa.get(index-bb);
							pre_extendedtext= node.text()+" "+pre_extendedtext;
						}
					}
					if (index+bb<aa.size()){
						temp = aa.get(index+bb).getClass().toString();
						if (temp.equals("class org.jsoup.nodes.TextNode")){
							post_extendedtext+= " "+aa.get(index+bb);
						} else if (temp.equals("class org.jsoup.nodes.Element")){
							node = (org.jsoup.nodes.Element) aa.get(index+bb);
							post_extendedtext+= " "+node.text();
						}
					}
					pre_extendedtext=pre_extendedtext.trim();
					stpre = new StringTokenizer(pre_extendedtext);
					countpre=stpre.countTokens();
					post_extendedtext=post_extendedtext.trim();
					stpost = new StringTokenizer(post_extendedtext);
					countpost=stpost.countTokens();
					count = countpre+countpost;
					if (count>=context_thresh){
						break;
					}
				}
				if (count==0){count=1;};
				if (countpre>=context_thresh & countpost>=context_thresh){
					prelimit= Math.min(countpre, (int) Math.rint(0.5*context_thresh));
					postlimit= Math.min(countpost,(int) Math.rint(0.5*context_thresh));
				}else{
					prelimit= Math.min(countpre,(int) Math.rint(countpre*context_thresh/count));
					postlimit= Math.min(countpost,(int) Math.rint(countpost*context_thresh/count));
				}
				preWORDS  = pre_extendedtext.split(" ");
				postWORDS = post_extendedtext.split(" ");
				for (int bb=preWORDS.length-prelimit-1;bb<preWORDS.length;bb++){
					if (bb>=0){
						wholetext = wholetext.concat(" "+ preWORDS[bb]);
					}
				}
				for (int bb=0;bb<postlimit;bb++){
					wholetext += " "+ postWORDS[bb];
				}
				rankedLinks[linksIndex] = new ExtendedOutlink(linktext,anchortext,wholetext);
				linksIndex++;
				//rankedlinks.add(new String[] {linktext, anchortext, wholetext});
			}		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rankedLinks;		
	}

}
