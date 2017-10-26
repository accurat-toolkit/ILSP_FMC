package gr.ilsp.fmc.utils;


import gr.ilsp.fmc.main.SimpleCrawlHFS;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;



@SuppressWarnings("deprecation")
public class TopicTools {
	private static Analyzer analyzer = null;
	private static AnalyzerFactory analyzerFactory = new AnalyzerFactory();
	//private static int MAX_CONTENT_TERMS = SimpleCrawlHFS.config.getInt("classifier.min_content_terms.value");
	//private static int MAX_CONTENT_TERMS = 5;

	public static ArrayList<String[]> analyzeTopic(String topicdef, String lang) {		
		//topicdef:filename of file with the topic definition
		//returns an array of strings with three columns (the triplets)
		//File temp=new File(topicdef);
		ArrayList<String[]> topic = new ArrayList<String[]>();
		Path p = new Path(topicdef);
		JobConf conf = new JobConf();
		conf.setJarByClass(SimpleCrawlHFS.class);
		//conf.set("hadoop.tmp.dir", "hadoop-temp");
		//conf.set("fs.default.name", "hdfs://kasos:54310/");
		//if (!temp.exists()){System.out.println("The file for topic definition does not exist.");}
		//else {
		//System.out.println("The file for topic definition exists.");
		
		try {
			FileSystem fs = FileSystem.get(conf);
			//Path p = new Path(fs.getWorkingDirectory()+"/"+topicdef);
			if (!fs.exists(p)) System.out.println("The file for topic definition does not exist.");
			else {
				BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(p),"UTF-8"));
				//BufferedReader in = new BufferedReader(new FileReader(temp));
				String str, a, b, c;

				while ((str = in.readLine()) != null) {
					a=str.subSequence(0, str.indexOf(":")).toString().replaceAll("\\W+","");			    	
					b=str.subSequence(str.indexOf(":")+1, str.indexOf("=")).toString().toLowerCase().trim();					
					ArrayList<String> stems = analyze(b, lang);
					b="";
					//concatenate stems
					for (String s:stems){ b=b.concat(" "+s);}
					b = b.trim();
					c=str.subSequence(str.indexOf("=")+1, str.length()).toString();
					topic.add(new String[] {a,b,c});
					
				}
				in.close();
				
			}
		} catch (IOException e) {e.printStackTrace();}

		//System.out.println("The topic definition contains: "+topic.size() +" terms.");
		return topic;
	}
	public static ArrayList<String> analyzeTopicALL(String topicdef) {
		//topicdef:filename of file with the topic definition
		//returns an array of strings with three columns (the triplets)
		File temp=new File(topicdef);
		ArrayList<String> topic = new ArrayList<String>();
		if (!temp.exists()){
			System.out.println("The file for topic definition does not exist.");
		}
		else {
			//System.out.println("The file for topic definition exists.");
			try {
				BufferedReader in = new BufferedReader(new FileReader(temp));
				String str, b;
				while ((str = in.readLine()) != null) {
					b=str.subSequence(str.indexOf(":")+1, str.indexOf("=")).toString();
					topic.add(b);
				}
				in.close();
			} catch (IOException e) {				
			}
		}
		System.out.println("The topic definition contains: "+topic.size() +" terms.");
		return topic;
	}
	public static ArrayList<String> analyze(String text, String lang) throws IOException  {
		ArrayList<String> stems = new ArrayList<String>();
		if (lang.equals("lv")) {
			stems = LatvianAnalyzer.analyze(text);
			//String totStem = "";
			//for (String s:ret){
			//	totStem = totStem + s + " ";
			//}    		
			//stems.add(totStem.trim()); //TODO: Convert LatvianAnalyzer to Lucene compatible analyzer
		}
		else if (lang.equals("lt")){
			stems = LithuanianAnalyzer.analyze(text);
		}
		else {
			try {
				analyzer = analyzerFactory.getAnalyzer(lang);
			} catch (Exception e) {
				//logger.fatal("Cannot initialize analyzer for lang " + lang);
				e.printStackTrace();
				return null;
			}
			TokenStream tokenStream = analyzer.tokenStream("contents", new StringReader(text));
			CharTermAttribute termAtt = (CharTermAttribute) tokenStream.addAttribute(CharTermAttribute.class);
			while (tokenStream.incrementToken()) {
				//logger.debug(termAtt.toString());
				stems.add(termAtt.toString());
			}
			tokenStream.close();
			analyzer.close();
		}
		return stems;
	}
	/*public static String[] findSubclasses(ArrayList<String[]> topic) {
		// gets the array with triplets and returns an array of strings with the subclasses
		// i.e. unique of third column of topic
		String[] temp = new String[topic.size ()];
		//String[] tempstr = new String[1];
		String[] tempstr = null;

		for (int ii=0;ii<topic.size();ii++){tempstr =topic.get(ii);	temp[ii]=tempstr[2];}
		Arrays.sort(temp);
		int k = 0;
		for (int i = 0; i < temp.length; i++){
			if (i > 0 && temp[i].equals(temp[i -1]))
				continue;
			temp[k++] = temp[i];
		}
		String[] classes = new String[k];
		System.arraycopy(temp, 0, classes, 0, k);
		//System.out.println("The topic definition contains: "+classes.length +" classes.");
		return classes;
	}*/
	public static String[] findSubclasses(ArrayList<String[]> topic) {
		// gets the array with triplets and returns an array of strings with the subclasses
		// i.e. unique of third column of topic
		ArrayList<String> temp = new ArrayList<String>();
		String[] tempstr = new String[1];
		String temp_line="";
		String[] subclasses;

		for (int ii=0;ii<topic.size();ii++){
			tempstr =topic.get(ii);	
			temp_line=tempstr[2];
			subclasses = temp_line.split(";");
			for (int kk=0;kk<subclasses.length;kk++){
				temp.add(subclasses[kk]);
			}
		}
		String[] new_temp = new String[temp.size()];
		for (int i = 0; i < new_temp.length; i++)
			new_temp[i] = temp.get(i);

		Arrays.sort(new_temp);
		int k = 0;
		for (int i = 0; i < new_temp.length; i++){
			if (i > 0 && new_temp[i].equals(new_temp[i -1]))
				continue;
			new_temp[k++] = new_temp[i];
		}
		String[] classes = new String[k];
		System.arraycopy(new_temp, 0, classes, 0, k);
		return classes;
	}
	public static double calculateThreshold(ArrayList<String[]> topic, int MAX_CONTENT_TERMS) {
		ArrayList<Double> temp1 = new ArrayList<Double>();
		double result=0.0;
		int kk=0;
		for (int ii=0;ii<topic.size();ii++){
			double s = Double.parseDouble(topic.get(ii)[0]);
			if (s>0){
				temp1.add(s);
				kk++;
			}	
		}
		if (temp1.size()==0)
			return 0;

		Double[] temp=new Double[kk];
		System.arraycopy(temp1.toArray(), 0, temp, 0, kk);
		Arrays.sort(temp);
		if (temp.length % 2 == 1) {
			result = temp[((temp.length+1)/2)-1];
		}else{
			result = (temp[((temp.length)/2)-1]+temp[(temp.length)/2])/2;
		}
		result=MAX_CONTENT_TERMS *result;
		//System.out.println("The threshold is: "+ result);
		return result;
	}

	public static String convertStreamToString(InputStream is)
	throws IOException {
		/*
		 * To convert the InputStream to String we use the
		 * Reader.read(char[] buffer) method. We iterate until the
		 * Reader return -1 which means there's no more data to
		 * read. We use the StringWriter class to produce the string.
		 */
		if (is != null) {
			Writer writer = new StringWriter();

			char[] buffer = new char[1024];
			try {
				Reader reader = new BufferedReader(
						new InputStreamReader(is, "UTF-8"));
				int n;
				while ((n = reader.read(buffer)) != -1) {
					writer.write(buffer, 0, n);
				}
			} finally {
				is.close();
			}
			return writer.toString();
		} else {       
			return "";
		}
	}

}
