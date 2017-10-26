package gr.ilsp.fmc.googlewide;

import gr.ilsp.fmc.main.SimpleCrawlHFS;

import java.util.ArrayList;

public class GoogleWidener {
	private static final int nrOfTerms = 4;
	public static ArrayList<String> getUrls(){
		ArrayList<String> urls = new ArrayList<String>();
		ArrayList<String[]> topic = SimpleCrawlHFS.getTopic();
		String query = "";
		for (int i=0;i<nrOfTerms;i++){
			
		}
		
		return urls;
	}
}
